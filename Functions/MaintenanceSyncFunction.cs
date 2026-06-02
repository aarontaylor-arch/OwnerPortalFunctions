using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace OwnerPortalFunctions.Functions;

public class MaintenanceSyncFunction(
    ILogger<MaintenanceSyncFunction> logger,
    IHttpClientFactory httpClientFactory)
{
    [Function("MaintenanceSyncFunction")]
    public async Task Run([TimerTrigger("0 */1 * * * *")] TimerInfo timer, CancellationToken cancellationToken)
    {
        logger.LogInformation("MaintenanceSyncFunction started at {Time}", DateTime.UtcNow);

        var dynamicsConnectionString = Environment.GetEnvironmentVariable("DynamicsConnectionString")
            ?? throw new InvalidOperationException("DynamicsConnectionString is not configured.");

        var sqlConnectionString = Environment.GetEnvironmentVariable("SqlConnectionString")
            ?? throw new InvalidOperationException("SqlConnectionString is not configured.");

        var dynamicsConfig = ParseDynamicsConnectionString(dynamicsConnectionString);
        var token = await GetDynamicsTokenAsync(dynamicsConfig, cancellationToken);
        var cases = await GetCasesRequiringApprovalAsync(dynamicsConfig.Url, token, cancellationToken);

        logger.LogInformation("Found {Count} cases requiring owner approval", cases.Count);

        foreach (var incident in cases)
        {
            await ProcessCaseAsync(incident, sqlConnectionString, cancellationToken);
        }

        await WriteApprovalsToDynamicsAsync(dynamicsConfig.Url, token, sqlConnectionString, cancellationToken);
    }

    private async Task ProcessCaseAsync(DynamicsCase incident, string sqlConnectionString, CancellationToken cancellationToken)
    {
        var customerName = incident.CustomerName;
        if (string.IsNullOrWhiteSpace(customerName))
        {
            logger.LogWarning("Case {IncidentId} has no customer name, skipping.", incident.IncidentId);
            return;
        }

        await using var connection = new SqlConnection(sqlConnectionString);
        await connection.OpenAsync(cancellationToken);

        var guestyId = await GetGuestyIdAsync(connection, customerName, cancellationToken);
        if (guestyId is null)
        {
            logger.LogWarning("No listing found for customer '{CustomerName}' on case {IncidentId}, skipping.", customerName, incident.IncidentId);
            return;
        }

        var ownerInfo = await GetOwnerInfoAsync(connection, guestyId, cancellationToken);
        if (ownerInfo is null)
        {
            logger.LogWarning("No active owner found for listing '{GuestyId}' (case {IncidentId}), skipping.", guestyId, incident.IncidentId);
            return;
        }

        logger.LogInformation(
            "Matched case {IncidentId} ('{Title}') to owner {Auth0UserId} via property {PropertyId}.",
            incident.IncidentId, incident.Title, ownerInfo.Auth0UserId, ownerInfo.PropertyId);

        await UpsertMaintenanceRequestAsync(connection, incident, ownerInfo, cancellationToken);
    }

    private static async Task UpsertMaintenanceRequestAsync(SqlConnection connection, DynamicsCase incident, OwnerInfo ownerInfo, CancellationToken cancellationToken)
    {
        const string countSql = "SELECT COUNT(*) FROM MaintenanceRequests WHERE DynamicsCaseId = @DynamicsCaseId";
        await using var countCmd = new SqlCommand(countSql, connection);
        countCmd.Parameters.AddWithValue("@DynamicsCaseId", incident.IncidentId);
        var count = (int)await countCmd.ExecuteScalarAsync(cancellationToken);

        var now = DateTime.UtcNow;

        if (count > 0)
        {
            const string updateSql = """
                UPDATE MaintenanceRequests
                SET CaseTitle = @CaseTitle, MessageToOwner = @MessageToOwner, UpdatedAt = @UpdatedAt
                WHERE DynamicsCaseId = @DynamicsCaseId
                """;
            await using var updateCmd = new SqlCommand(updateSql, connection);
            updateCmd.Parameters.AddWithValue("@CaseTitle", incident.Title);
            updateCmd.Parameters.AddWithValue("@MessageToOwner", (object?)incident.MessageToOwner ?? DBNull.Value);
            updateCmd.Parameters.AddWithValue("@UpdatedAt", now);
            updateCmd.Parameters.AddWithValue("@DynamicsCaseId", incident.IncidentId);
            await updateCmd.ExecuteNonQueryAsync(cancellationToken);
        }
        else
        {
            const string insertSql = """
                INSERT INTO MaintenanceRequests (DynamicsCaseId, CaseTitle, MessageToOwner, Auth0UserId, PropertyId, CreatedAt, UpdatedAt)
                VALUES (@DynamicsCaseId, @CaseTitle, @MessageToOwner, @Auth0UserId, @PropertyId, @CreatedAt, @UpdatedAt)
                """;
            await using var insertCmd = new SqlCommand(insertSql, connection);
            insertCmd.Parameters.AddWithValue("@DynamicsCaseId", incident.IncidentId);
            insertCmd.Parameters.AddWithValue("@CaseTitle", incident.Title);
            insertCmd.Parameters.AddWithValue("@MessageToOwner", (object?)incident.MessageToOwner ?? DBNull.Value);
            insertCmd.Parameters.AddWithValue("@Auth0UserId", ownerInfo.Auth0UserId);
            insertCmd.Parameters.AddWithValue("@PropertyId", ownerInfo.PropertyId);
            insertCmd.Parameters.AddWithValue("@CreatedAt", now);
            insertCmd.Parameters.AddWithValue("@UpdatedAt", now);
            await insertCmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task<string?> GetGuestyIdAsync(SqlConnection connection, string customerName, CancellationToken cancellationToken)
    {
        const string sql = "SELECT GuestyId FROM Listings WHERE NickName = @NickName";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@NickName", customerName);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result as string;
    }

    private static async Task<OwnerInfo?> GetOwnerInfoAsync(SqlConnection connection, string guestyId, CancellationToken cancellationToken)
    {
        const string sql = "SELECT Auth0UserId, PropertyId FROM OwnerProperties WHERE PropertyId = @PropertyId AND IsActive = 1";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@PropertyId", guestyId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new OwnerInfo(
                Auth0UserId: reader.GetString(reader.GetOrdinal("Auth0UserId")),
                PropertyId: reader.GetString(reader.GetOrdinal("PropertyId")));
        }
        return null;
    }

    private async Task<List<DynamicsCase>> GetCasesRequiringApprovalAsync(string dynamicsUrl, string token, CancellationToken cancellationToken)
    {
        var client = httpClientFactory.CreateClient();
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        client.DefaultRequestHeaders.Add("OData-MaxVersion", "4.0");
        client.DefaultRequestHeaders.Add("OData-Version", "4.0");
        client.DefaultRequestHeaders.Add("Prefer", "odata.include-annotations=\"OData.Community.Display.V1.FormattedValue\"");

        var url = $"{dynamicsUrl.TrimEnd('/')}/api/data/v9.2/incidents?$select=incidentid,title,crd9b_messagetoowner,crd9b_ownerapprovalstatus,crd9b_requiresownerapproval,createdon,_customerid_value&$filter=crd9b_requiresownerapproval%20eq%20true%20and%20crd9b_ownerapprovalstatus%20eq%20915370001&$top=5&$orderby=createdon%20desc";

        var response = await client.GetAsync(url, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException(
                $"Dynamics cases request failed with status {(int)response.StatusCode} ({response.StatusCode}): {errorBody}");
        }

        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        logger.LogInformation("Dynamics raw response: {RawResponse}", content);
        var result = JsonSerializer.Deserialize<DynamicsResponse>(content, JsonOptions)
            ?? throw new InvalidOperationException("Failed to deserialize Dynamics response.");

        return result.Value;
    }

    private async Task<string> GetDynamicsTokenAsync(DynamicsConfig config, CancellationToken cancellationToken)
    {
        var client = httpClientFactory.CreateClient();

        var form = new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = config.ClientId,
            ["client_secret"] = config.ClientSecret,
            ["resource"] = config.Url
        };

        var tenantId = Environment.GetEnvironmentVariable("DynamicsTenantId")
            ?? throw new InvalidOperationException("DynamicsTenantId is not configured.");

        var response = await client.PostAsync(
            $"https://login.microsoftonline.com/{tenantId}/oauth2/token",
            new FormUrlEncodedContent(form),
            cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException(
                $"Token request failed with status {(int)response.StatusCode} ({response.StatusCode}): {errorBody}");
        }

        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(content, JsonOptions)
            ?? throw new InvalidOperationException("Failed to deserialize token response.");

        return tokenResponse.AccessToken;
    }

    private async Task WriteApprovalsToDynamicsAsync(string dynamicsUrl, string token, string sqlConnectionString, CancellationToken cancellationToken)
    {
        const string selectSql = """
            SELECT Id, DynamicsCaseId, Status, OwnerComments
            FROM MaintenanceRequests
            WHERE (Status = 'Approved' OR Status = 'Declined')
            AND SyncedToDynamics = 0
            """;

        List<PendingApproval> pending = [];

        await using (var connection = new SqlConnection(sqlConnectionString))
        {
            await connection.OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(selectSql, connection);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                pending.Add(new PendingApproval(
                    Id: reader.GetInt32(reader.GetOrdinal("Id")),
                    DynamicsCaseId: reader.GetString(reader.GetOrdinal("DynamicsCaseId")),
                    Status: reader.GetString(reader.GetOrdinal("Status")),
                    OwnerComments: reader.IsDBNull(reader.GetOrdinal("OwnerComments")) ? null : reader.GetString(reader.GetOrdinal("OwnerComments"))));
            }
        }

        logger.LogInformation("Found {Count} approval decisions to write back to Dynamics", pending.Count);

        var client = httpClientFactory.CreateClient();
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        client.DefaultRequestHeaders.Add("OData-MaxVersion", "4.0");
        client.DefaultRequestHeaders.Add("OData-Version", "4.0");

        foreach (var approval in pending)
        {
            try
            {
                var statusCode = approval.Status == "Approved" ? 915370002 : 915370003;
                var body = JsonSerializer.Serialize(new
                {
                    crd9b_ownerapprovalstatus = statusCode,
                    crd9b_ownercomments = approval.OwnerComments
                });

                var patchUrl = $"{dynamicsUrl.TrimEnd('/')}/api/data/v9.2/incidents({approval.DynamicsCaseId})";
                var request = new HttpRequestMessage(HttpMethod.Patch, patchUrl)
                {
                    Content = new StringContent(body, System.Text.Encoding.UTF8, "application/json")
                };

                var response = await client.SendAsync(request, cancellationToken);

                if (response.StatusCode != System.Net.HttpStatusCode.NoContent)
                {
                    var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);
                    logger.LogError(
                        "Failed to PATCH Dynamics case {DynamicsCaseId} for MaintenanceRequest {Id}: {Status} {Body}",
                        approval.DynamicsCaseId, approval.Id, (int)response.StatusCode, errorBody);
                    continue;
                }

                await using var connection = new SqlConnection(sqlConnectionString);
                await connection.OpenAsync(cancellationToken);
                const string updateSql = "UPDATE MaintenanceRequests SET SyncedToDynamics = 1, UpdatedAt = GETUTCDATE() WHERE Id = @Id";
                await using var updateCmd = new SqlCommand(updateSql, connection);
                updateCmd.Parameters.AddWithValue("@Id", approval.Id);
                await updateCmd.ExecuteNonQueryAsync(cancellationToken);

                logger.LogInformation(
                    "Successfully wrote {Status} decision for MaintenanceRequest {Id} (Dynamics case {DynamicsCaseId})",
                    approval.Status, approval.Id, approval.DynamicsCaseId);
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Unexpected error writing approval for MaintenanceRequest {Id} (Dynamics case {DynamicsCaseId})",
                    approval.Id, approval.DynamicsCaseId);
            }
        }
    }

    private static DynamicsConfig ParseDynamicsConnectionString(string connectionString)
    {
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var part in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var idx = part.IndexOf('=');
            if (idx > 0)
                dict[part[..idx].Trim()] = part[(idx + 1)..].Trim();
        }

        if (!dict.TryGetValue("url", out var url) || string.IsNullOrWhiteSpace(url))
            throw new InvalidOperationException("DynamicsConnectionString is missing 'url'.");
        if (!dict.TryGetValue("ClientId", out var clientId) || string.IsNullOrWhiteSpace(clientId))
            throw new InvalidOperationException("DynamicsConnectionString is missing 'ClientId'.");
        if (!dict.TryGetValue("ClientSecret", out var clientSecret) || string.IsNullOrWhiteSpace(clientSecret))
            throw new InvalidOperationException("DynamicsConnectionString is missing 'ClientSecret'.");

        return new DynamicsConfig(url, clientId, clientSecret);
    }

    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private sealed record DynamicsConfig(string Url, string ClientId, string ClientSecret);
    private sealed record OwnerInfo(string Auth0UserId, string PropertyId);
    private sealed record PendingApproval(int Id, string DynamicsCaseId, string Status, string? OwnerComments);

    private sealed class DynamicsResponse
    {
        [JsonPropertyName("value")]
        public List<DynamicsCase> Value { get; set; } = [];
    }

    private sealed class DynamicsCase
    {
        [JsonPropertyName("incidentid")]
        public string IncidentId { get; set; } = "";

        [JsonPropertyName("title")]
        public string Title { get; set; } = "";

        [JsonPropertyName("crd9b_messagetoowner")]
        public string? MessageToOwner { get; set; }

        [JsonPropertyName("crd9b_ownerapprovalstatus")]
        public int? OwnerApprovalStatus { get; set; }

        [JsonPropertyName("createdon")]
        public DateTime CreatedOn { get; set; }

        [JsonPropertyName("_customerid_value")]
        public string? CustomerId { get; set; }

        [JsonPropertyName("_customerid_value@OData.Community.Display.V1.FormattedValue")]
        public string? CustomerName { get; set; }
    }

    private sealed class TokenResponse
    {
        [JsonPropertyName("access_token")]
        public string AccessToken { get; set; } = "";
    }
}
