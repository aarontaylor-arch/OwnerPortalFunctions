using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace OwnerPortalFunctions.Functions;

public class StatementSyncFunction(
    ILogger<StatementSyncFunction> logger,
    IHttpClientFactory httpClientFactory)
{
    private const string TenantId = "0b8a5353-fc23-4faf-94cc-be0157c522e0";
    private const string MailboxUser = "clients@bnbmadeeasy.com.au";

    [Function("StatementSyncFunction")]
    public async Task Run([TimerTrigger("0 */10 * * * *")] TimerInfo timer, CancellationToken cancellationToken)
    {
        logger.LogInformation("StatementSyncFunction started at {Time}", DateTime.UtcNow);

        var sqlConnectionString = Environment.GetEnvironmentVariable("SqlConnectionString")
            ?? throw new InvalidOperationException("SqlConnectionString is not configured.");

        var graphClientId = Environment.GetEnvironmentVariable("GraphClientId")
            ?? throw new InvalidOperationException("GraphClientId is not configured.");

        var graphClientSecret = Environment.GetEnvironmentVariable("GraphClientSecret")
            ?? throw new InvalidOperationException("GraphClientSecret is not configured.");

        var blobConnectionString = Environment.GetEnvironmentVariable("BlobStorageConnectionString")
            ?? throw new InvalidOperationException("BlobStorageConnectionString is not configured.");

        var graphToken = await GetGraphTokenAsync(graphClientId, graphClientSecret, cancellationToken);
        var messages = await GetDisbursementEmailsAsync(graphToken, cancellationToken);

        logger.LogInformation("Found {Count} 'Disbursement Report' emails", messages.Count);

        var imported = 0;

        foreach (var message in messages)
        {
            try
            {
                var wasImported = await ProcessMessageAsync(message, graphToken, sqlConnectionString, blobConnectionString, cancellationToken);
                if (wasImported) imported++;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unexpected error processing email {MessageId}: {ExceptionMessage}", message.Id, ex.Message);
            }
        }

        logger.LogInformation("StatementSyncFunction complete. Imported {Count} new statements.", imported);
    }

    private async Task<bool> ProcessMessageAsync(
        GraphMessage message,
        string graphToken,
        string sqlConnectionString,
        string blobConnectionString,
        CancellationToken cancellationToken)
    {
        await using var connection = new SqlConnection(sqlConnectionString);
        await connection.OpenAsync(cancellationToken);

        if (await StatementExistsAsync(connection, message.Id, cancellationToken))
            return false;

        var attachments = await GetAttachmentsAsync(message.Id, graphToken, cancellationToken);
        var pdfAttachment = attachments.FirstOrDefault(a =>
            string.Equals(a.Name, "bookingTrust.pdf", StringComparison.OrdinalIgnoreCase));

        if (pdfAttachment is null)
        {
            logger.LogWarning("No bookingTrust.pdf attachment found for email {MessageId} (subject: '{Subject}')", message.Id, message.Subject);
            return false;
        }

        const string prefix = "Disbursement Report for ";
        if (!message.Subject.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            logger.LogWarning("Email {MessageId} subject '{Subject}' does not match expected format, skipping.", message.Id, message.Subject);
            return false;
        }

        var propertyName = message.Subject[prefix.Length..].Trim();

        var propertyId = await GetPropertyIdAsync(connection, propertyName, cancellationToken);
        if (propertyId is null)
        {
            logger.LogWarning("No listing found for property '{PropertyName}' (email {MessageId}), skipping.", propertyName, message.Id);
            return false;
        }

        var statementDate = new DateTime(message.ReceivedDateTime.Year, message.ReceivedDateTime.Month, 1);
        var pdfBytes = Convert.FromBase64String(pdfAttachment.ContentBytes);

        var blobPath = $"{propertyId}/{statementDate:yyyy-MM}/bookingTrust.pdf";
        var blobUrl = await UploadToBlobAsync(blobConnectionString, blobPath, pdfBytes, cancellationToken);

        await InsertStatementAsync(connection, propertyId, propertyName, statementDate, blobUrl, message.Id, "bookingTrust.pdf", cancellationToken);

        logger.LogInformation("Imported statement for {PropertyName} ({StatementDate:yyyy-MM-dd})", propertyName, statementDate);
        return true;
    }

    private static async Task<bool> StatementExistsAsync(SqlConnection connection, string graphMessageId, CancellationToken cancellationToken)
    {
        const string sql = "SELECT COUNT(*) FROM Statements WHERE GraphMessageId = @GraphMessageId";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@GraphMessageId", graphMessageId);
        var count = (int)await command.ExecuteScalarAsync(cancellationToken);
        return count > 0;
    }

    private static async Task<string?> GetPropertyIdAsync(SqlConnection connection, string propertyName, CancellationToken cancellationToken)
    {
        const string sql = "SELECT GuestyId FROM Listings WHERE ListingName = @ListingName";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@ListingName", propertyName);
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result as string;
    }

    private static async Task<string> UploadToBlobAsync(string connectionString, string blobPath, byte[] data, CancellationToken cancellationToken)
    {
        var containerClient = new BlobContainerClient(connectionString, "owner-statements");
        await containerClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken);

        var blobClient = containerClient.GetBlobClient(blobPath);
        using var stream = new MemoryStream(data);
        await blobClient.UploadAsync(stream, overwrite: true, cancellationToken: cancellationToken);

        return blobClient.Uri.ToString();
    }

    private static async Task InsertStatementAsync(
        SqlConnection connection,
        string propertyId,
        string propertyName,
        DateTime statementDate,
        string blobUrl,
        string graphMessageId,
        string fileName,
        CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO Statements (PropertyId, PropertyName, StatementDate, BlobUrl, GraphMessageId, FileName, CreatedAt)
            VALUES (@PropertyId, @PropertyName, @StatementDate, @BlobUrl, @GraphMessageId, @FileName, GETUTCDATE())
            """;
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@PropertyId", propertyId);
        command.Parameters.AddWithValue("@PropertyName", propertyName);
        command.Parameters.AddWithValue("@StatementDate", statementDate);
        command.Parameters.AddWithValue("@BlobUrl", blobUrl);
        command.Parameters.AddWithValue("@GraphMessageId", graphMessageId);
        command.Parameters.AddWithValue("@FileName", fileName);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<List<GraphMessage>> GetDisbursementEmailsAsync(string graphToken, CancellationToken cancellationToken)
    {
        var client = httpClientFactory.CreateClient();
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", graphToken);

        const string url = $"https://graph.microsoft.com/v1.0/users/{MailboxUser}/messages" +
                           "?$filter=contains(subject,'Disbursement Report')" +
                           "&$select=id,subject,receivedDateTime&$top=50";

        var response = await client.GetAsync(url, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException($"Graph messages request failed with status {(int)response.StatusCode}: {errorBody}");
        }

        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        var result = JsonSerializer.Deserialize<GraphMessagesResponse>(content, JsonOptions)
            ?? throw new InvalidOperationException("Failed to deserialize Graph messages response.");

        return result.Value;
    }

    private async Task<List<GraphAttachment>> GetAttachmentsAsync(string messageId, string graphToken, CancellationToken cancellationToken)
    {
        var client = httpClientFactory.CreateClient();
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", graphToken);

        var url = $"https://graph.microsoft.com/v1.0/users/{MailboxUser}/messages/{messageId}/attachments";

        var response = await client.GetAsync(url, cancellationToken);
        if (!response.IsSuccessStatusCode)
        {
            var errorBody = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException($"Graph attachments request failed with status {(int)response.StatusCode}: {errorBody}");
        }

        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        var result = JsonSerializer.Deserialize<GraphAttachmentsResponse>(content, JsonOptions)
            ?? throw new InvalidOperationException("Failed to deserialize Graph attachments response.");

        return result.Value;
    }

    private async Task<string> GetGraphTokenAsync(string clientId, string clientSecret, CancellationToken cancellationToken)
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
            $"https://login.microsoftonline.com/{TenantId}/oauth2/v2.0/token",
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

    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private sealed class GraphMessagesResponse
    {
        [JsonPropertyName("value")]
        public List<GraphMessage> Value { get; set; } = [];
    }

    private sealed class GraphMessage
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = "";

        [JsonPropertyName("subject")]
        public string Subject { get; set; } = "";

        [JsonPropertyName("receivedDateTime")]
        public DateTime ReceivedDateTime { get; set; }
    }

    private sealed class GraphAttachmentsResponse
    {
        [JsonPropertyName("value")]
        public List<GraphAttachment> Value { get; set; } = [];
    }

    private sealed class GraphAttachment
    {
        [JsonPropertyName("name")]
        public string Name { get; set; } = "";

        [JsonPropertyName("contentBytes")]
        public string ContentBytes { get; set; } = "";
    }

    private sealed class TokenResponse
    {
        [JsonPropertyName("access_token")]
        public string AccessToken { get; set; } = "";
    }
}
