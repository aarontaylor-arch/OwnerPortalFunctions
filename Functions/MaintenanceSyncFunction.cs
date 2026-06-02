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

        var isNew = await UpsertMaintenanceRequestAsync(connection, incident, ownerInfo, cancellationToken);

        if (isNew)
        {
            await SendOwnerEmailAsync(incident, ownerInfo, customerName, cancellationToken);
        }
    }

    private static async Task<bool> UpsertMaintenanceRequestAsync(SqlConnection connection, DynamicsCase incident, OwnerInfo ownerInfo, CancellationToken cancellationToken)
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
                SET CaseTitle = @CaseTitle, CaseNumber = @CaseNumber, MessageToOwner = @MessageToOwner, UpdatedAt = @UpdatedAt
                WHERE DynamicsCaseId = @DynamicsCaseId
                """;
            await using var updateCmd = new SqlCommand(updateSql, connection);
            updateCmd.Parameters.AddWithValue("@CaseTitle", incident.Title);
            updateCmd.Parameters.AddWithValue("@CaseNumber", (object?)incident.CaseNumber ?? DBNull.Value);
            updateCmd.Parameters.AddWithValue("@MessageToOwner", (object?)incident.MessageToOwner ?? DBNull.Value);
            updateCmd.Parameters.AddWithValue("@UpdatedAt", now);
            updateCmd.Parameters.AddWithValue("@DynamicsCaseId", incident.IncidentId);
            await updateCmd.ExecuteNonQueryAsync(cancellationToken);
            return false;
        }
        else
        {
            const string insertSql = """
                INSERT INTO MaintenanceRequests (DynamicsCaseId, CaseTitle, CaseNumber, MessageToOwner, Auth0UserId, PropertyId, CreatedAt, UpdatedAt)
                VALUES (@DynamicsCaseId, @CaseTitle, @CaseNumber, @MessageToOwner, @Auth0UserId, @PropertyId, @CreatedAt, @UpdatedAt)
                """;
            await using var insertCmd = new SqlCommand(insertSql, connection);
            insertCmd.Parameters.AddWithValue("@DynamicsCaseId", incident.IncidentId);
            insertCmd.Parameters.AddWithValue("@CaseTitle", incident.Title);
            insertCmd.Parameters.AddWithValue("@CaseNumber", (object?)incident.CaseNumber ?? DBNull.Value);
            insertCmd.Parameters.AddWithValue("@MessageToOwner", (object?)incident.MessageToOwner ?? DBNull.Value);
            insertCmd.Parameters.AddWithValue("@Auth0UserId", ownerInfo.Auth0UserId);
            insertCmd.Parameters.AddWithValue("@PropertyId", ownerInfo.PropertyId);
            insertCmd.Parameters.AddWithValue("@CreatedAt", now);
            insertCmd.Parameters.AddWithValue("@UpdatedAt", now);
            await insertCmd.ExecuteNonQueryAsync(cancellationToken);
            return true;
        }
    }

    private async Task SendOwnerEmailAsync(DynamicsCase incident, OwnerInfo ownerInfo, string propertyName, CancellationToken cancellationToken)
    {
        try
        {
            var ownerEmail = await GetOwnerEmailFromAuth0Async(ownerInfo.Auth0UserId, cancellationToken);
            if (ownerEmail is null)
            {
                logger.LogWarning("Could not retrieve email for Auth0 user {Auth0UserId}, skipping owner email.", ownerInfo.Auth0UserId);
                return;
            }

            var tenantId = Environment.GetEnvironmentVariable("DynamicsTenantId")
                ?? throw new InvalidOperationException("DynamicsTenantId is not configured.");
            var graphClientId = Environment.GetEnvironmentVariable("GraphClientId")
                ?? throw new InvalidOperationException("GraphClientId is not configured.");
            var graphClientSecret = Environment.GetEnvironmentVariable("GraphClientSecret")
                ?? throw new InvalidOperationException("GraphClientSecret is not configured.");

            var graphToken = await GetGraphTokenAsync(tenantId, graphClientId, graphClientSecret, cancellationToken);

            var subject = $"Action Required: Maintenance approval needed for {propertyName}";
            var body = $"A new maintenance request requires your approval.\n\nProperty: {propertyName}\nCase: {incident.CaseNumber} - {incident.Title}\n\nMessage from our team:\n{incident.MessageToOwner}\n\nPlease log in to your Owner Portal to approve or decline:\nhttps://honey-homes-owner-portal.azurewebsites.net";

            var emailPayload = new
            {
                message = new
                {
                    subject,
                    body = new { contentType = "Text", content = body },
                    toRecipients = new[] { new { emailAddress = new { address = ownerEmail } } }
                }
            };

            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", graphToken);

            var json = JsonSerializer.Serialize(emailPayload);
            var response = await client.PostAsync(
                "https://graph.microsoft.com/v1.0/users/clients@bnbmadeeasy.com.au/sendMail",
                new StringContent(json, System.Text.Encoding.UTF8, "application/json"),
                cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);
                logger.LogError("Failed to send owner email for case {IncidentId}: {Status} {Body}", incident.IncidentId, (int)response.StatusCode, errorBody);
                return;
            }

            logger.LogInformation("Sent owner approval email to {OwnerEmail} for case {IncidentId}", ownerEmail, incident.IncidentId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error sending owner email for case {IncidentId}", incident.IncidentId);
        }
    }

    private async Task<string?> GetOwnerEmailFromAuth0Async(string auth0UserId, CancellationToken cancellationToken)
    {
        var clientId = Environment.GetEnvironmentVariable("Auth0ManagementClientId")
            ?? throw new InvalidOperationException("Auth0ManagementClientId is not configured.");
        var clientSecret = Environment.GetEnvironmentVariable("Auth0ManagementClientSecret")
            ?? throw new InvalidOperationException("Auth0ManagementClientSecret is not configured.");

        var client = httpClientFactory.CreateClient();

        var tokenForm = new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = clientId,
            ["client_secret"] = clientSecret,
            ["audience"] = "https://honeyhomes-owner-portal.au.auth0.com/api/v2/"
        };

        var tokenResponse = await client.PostAsync(
            "https://honeyhomes-owner-portal.au.auth0.com/oauth/token",
            new FormUrlEncodedContent(tokenForm),
            cancellationToken);

        if (!tokenResponse.IsSuccessStatusCode)
        {
            var errorBody = await tokenResponse.Content.ReadAsStringAsync(cancellationToken);
            logger.LogError("Failed to get Auth0 management token: {Status} {Body}", (int)tokenResponse.StatusCode, errorBody);
            return null;
        }

        var tokenContent = await tokenResponse.Content.ReadAsStringAsync(cancellationToken);
        var tokenData = JsonSerializer.Deserialize<Auth0TokenResponse>(tokenContent, JsonOptions);
        if (tokenData?.AccessToken is null)
        {
            logger.LogError("Auth0 management token response missing access_token");
            return null;
        }

        var encodedUserId = Uri.EscapeDataString(auth0UserId);
        var userRequest = new HttpRequestMessage(HttpMethod.Get, $"https://honeyhomes-owner-portal.au.auth0.com/api/v2/users/{encodedUserId}");
        userRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", tokenData.AccessToken);

        var userResponse = await client.SendAsync(userRequest, cancellationToken);
        if (!userResponse.IsSuccessStatusCode)
        {
            var errorBody = await userResponse.Content.ReadAsStringAsync(cancellationToken);
            logger.LogError("Failed to get Auth0 user {UserId}: {Status} {Body}", auth0UserId, (int)userResponse.StatusCode, errorBody);
            return null;
        }

        var userContent = await userResponse.Content.ReadAsStringAsync(cancellationToken);
        var user = JsonSerializer.Deserialize<Auth0User>(userContent, JsonOptions);
        return user?.Email;
    }

    private async Task<string> GetGraphTokenAsync(string tenantId, string clientId, string clientSecret, CancellationToken cancellationToken)
    {
        var client = httpClientFactory.CreateClient();

        var form = new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = clientId,
            ["client_secret"] = clientSecret,
            ["scope"] = "https://graph.microsoft.com/.default"
        };

        var response = await client.PostAsync(
            $"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token",
            new FormUrlEncodedContent(form),
            cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException($"Graph token request failed with status {(int)response.StatusCode}: {errorBody}");
        }

        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        var tokenResponse = JsonSerializer.Deserialize<TokenResponse>(content, JsonOptions)
            ?? throw new InvalidOperationException("Failed to deserialize Graph token response.");

        return tokenResponse.AccessToken;
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

        var url = $"{dynamicsUrl.TrimEnd('/')}/api/data/v9.2/incidents?$select=incidentid,title,ticketnumber,crd9b_messagetoowner,crd9b_ownerapprovalstatus,crd9b_requiresownerapproval,createdon,_customerid_value&$filter=crd9b_requiresownerapproval%20eq%20true%20and%20crd9b_ownerapprovalstatus%20eq%20915370001&$top=5&$orderby=createdon%20desc";

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
            SELECT Id, DynamicsCaseId, Status, OwnerComments, PropertyId, CaseTitle, CaseNumber
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
                    OwnerComments: reader.IsDBNull(reader.GetOrdinal("OwnerComments")) ? null : reader.GetString(reader.GetOrdinal("OwnerComments")),
                    PropertyId: reader.GetString(reader.GetOrdinal("PropertyId")),
                    CaseTitle: reader.GetString(reader.GetOrdinal("CaseTitle")),
                    CaseNumber: reader.IsDBNull(reader.GetOrdinal("CaseNumber")) ? null : reader.GetString(reader.GetOrdinal("CaseNumber"))));
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

                var propertyName = await GetPropertyNameAsync(connection, approval.PropertyId, cancellationToken);
                await SendSlackNotificationAsync(approval, propertyName, cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "Unexpected error writing approval for MaintenanceRequest {Id} (Dynamics case {DynamicsCaseId})",
                    approval.Id, approval.DynamicsCaseId);
            }
        }
    }

    private static async Task<string> GetPropertyNameAsync(SqlConnection connection, string propertyId, CancellationToken cancellationToken)
    {
        const string sql = "SELECT NickName FROM Listings WHERE GuestyId = @GuestyId";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@GuestyId", propertyId);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result as string ?? propertyId;
    }

    private async Task SendSlackNotificationAsync(PendingApproval approval, string propertyName, CancellationToken cancellationToken)
    {
        var webhookUrl = Environment.GetEnvironmentVariable("OwnerPortalSlackWebhookUrl");
        if (string.IsNullOrWhiteSpace(webhookUrl))
        {
            logger.LogWarning("OwnerPortalSlackWebhookUrl is not configured, skipping Slack notification.");
            return;
        }

        try
        {
            var text = $"Owner has {approval.Status} a maintenance request.\n*Property:* {propertyName}\n*Case:* {approval.CaseNumber} - {approval.CaseTitle}\n*Decision:* {approval.Status}\n*Comment:* {approval.OwnerComments}";
            var payload = JsonSerializer.Serialize(new { text });

            var slackClient = httpClientFactory.CreateClient();
            var response = await slackClient.PostAsync(
                webhookUrl,
                new StringContent(payload, System.Text.Encoding.UTF8, "application/json"),
                cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);
                logger.LogError("Failed to send Slack notification for MaintenanceRequest {Id}: {Status} {Body}", approval.Id, (int)response.StatusCode, errorBody);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error sending Slack notification for MaintenanceRequest {Id}", approval.Id);
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
    private sealed record PendingApproval(int Id, string DynamicsCaseId, string Status, string? OwnerComments, string PropertyId, string CaseTitle, string? CaseNumber);

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

        [JsonPropertyName("ticketnumber")]
        public string? CaseNumber { get; set; }

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

    private sealed class Auth0TokenResponse
    {
        [JsonPropertyName("access_token")]
        public string? AccessToken { get; set; }
    }

    private sealed class Auth0User
    {
        [JsonPropertyName("email")]
        public string? Email { get; set; }
    }
}
