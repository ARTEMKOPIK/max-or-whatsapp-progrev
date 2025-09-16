using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace MaxTelegramBot
{
	public class InvoiceInfo
	{
		public string Url { get; set; } = string.Empty;
		public string Hash { get; set; } = string.Empty;
	}

	public class TransferInfo
	{
		public long TransferId { get; set; }
		public string SpendId { get; set; } = string.Empty;
		public string Status { get; set; } = string.Empty;
		public string CompletedAt { get; set; } = string.Empty;
	}

	public class CheckInfo
	{
		public long CheckId { get; set; }
		public string Hash { get; set; } = string.Empty;
		public string BotCheckUrl { get; set; } = string.Empty;
		public string Status { get; set; } = string.Empty;
		public string CreatedAt { get; set; } = string.Empty;
	}

	public class CryptoPayService
	{
		private readonly HttpClient _httpClient;
		private readonly string _apiToken;

		public CryptoPayService(string apiToken)
		{
			_apiToken = apiToken ?? string.Empty;
			_httpClient = new HttpClient
			{
				BaseAddress = new Uri("https://pay.crypt.bot/api/")
			};
			_httpClient.DefaultRequestHeaders.Add("Crypto-Pay-API-Token", _apiToken);
		}

		public async Task<InvoiceInfo?> CreateInvoiceAsync(decimal amount, string asset, string description)
		{
			var payload = new
			{
				amount = amount.ToString(CultureInfo.InvariantCulture),
				asset = asset,
				description = description
			};

			var json = JsonConvert.SerializeObject(payload);
			var content = new StringContent(json, Encoding.UTF8, "application/json");

			HttpResponseMessage response;
			try
			{
				response = await _httpClient.PostAsync("createInvoice", content);
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Ошибка соединения с Crypto Pay API: {ex.Message}");
				return null;
			}

			var body = await response.Content.ReadAsStringAsync();
			Console.WriteLine($"CryptoPay createInvoice status {(int)response.StatusCode}: {body}");

			if (!response.IsSuccessStatusCode)
				return null;

			try
			{
				var obj = JObject.Parse(body);
				var result = obj["result"];
				var miniAppInvoiceUrl = result?["mini_app_invoice_url"]?.ToString();
				var hash = result?["hash"]?.ToString();
				var payUrl = result?["pay_url"]?.ToString();

				var url = !string.IsNullOrEmpty(miniAppInvoiceUrl)
					? miniAppInvoiceUrl
					: (!string.IsNullOrEmpty(hash) ? $"https://t.me/CryptoBot/app?startapp=invoice-{hash}&mode=compact" : payUrl);

				return new InvoiceInfo { Url = url ?? string.Empty, Hash = hash ?? string.Empty };
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Ошибка парсинга ответа Crypto Pay: {ex.Message}");
				return null;
			}
		}

                public async Task<string?> GetInvoiceStatusAsync(string hash)
                {
                        try
                        {
                                var response = await _httpClient.GetAsync($"getInvoices?hash={Uri.EscapeDataString(hash)}");
                                var body = await response.Content.ReadAsStringAsync();
                                Console.WriteLine($"CryptoPay getInvoices status {(int)response.StatusCode}: {body}");
                                if (!response.IsSuccessStatusCode) return null;

                                var obj = JObject.Parse(body);
                                var result = obj["result"];
                                JToken? item = null;

                                if (result?["items"] is JArray items)
                                {
                                        item = items.FirstOrDefault(i =>
                                                string.Equals(i?["hash"]?.ToString(), hash, StringComparison.OrdinalIgnoreCase));
                                }
                                else if (string.Equals(result?["hash"]?.ToString(), hash, StringComparison.OrdinalIgnoreCase))
                                {
                                        item = result;
                                }

                                var status = item?["status"]?.ToString();
                                return status;
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine($"Ошибка GetInvoiceStatusAsync: {ex.Message}");
                                return null;
                        }
                }

		// Автоматический перевод средств пользователю
		public async Task<TransferInfo?> TransferAsync(string userId, decimal amount, string asset, string comment = "")
		{
			var payload = new
			{
				user_id = userId,
				asset = asset,
				amount = amount.ToString(CultureInfo.InvariantCulture),
				comment = comment
			};

			var json = JsonConvert.SerializeObject(payload);
			var content = new StringContent(json, Encoding.UTF8, "application/json");

			HttpResponseMessage response;
			try
			{
				response = await _httpClient.PostAsync("transfer", content);
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Ошибка соединения с Crypto Pay API при переводе: {ex.Message}");
				return null;
			}

			var body = await response.Content.ReadAsStringAsync();
			Console.WriteLine($"CryptoPay transfer status {(int)response.StatusCode}: {body}");

			if (!response.IsSuccessStatusCode)
			{
				Console.WriteLine($"Ошибка перевода: {body}");
				return null;
			}

			try
			{
				var obj = JObject.Parse(body);
				var result = obj["result"];
				
				return new TransferInfo
				{
					TransferId = result?["transfer_id"]?.Value<long>() ?? 0,
					SpendId = result?["spend_id"]?.ToString() ?? string.Empty,
					Status = result?["status"]?.ToString() ?? string.Empty,
					CompletedAt = result?["completed_at"]?.ToString() ?? string.Empty
				};
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Ошибка парсинга ответа перевода: {ex.Message}");
				return null;
			}
		}

		// Создание чека для выплаты
		public async Task<CheckInfo?> CreateCheckAsync(decimal amount, string asset, string comment = "")
		{
			var payload = new
			{
				asset = asset,
				amount = amount.ToString(CultureInfo.InvariantCulture),
				comment = comment
			};

			var json = JsonConvert.SerializeObject(payload);
			var content = new StringContent(json, Encoding.UTF8, "application/json");

			HttpResponseMessage response;
			try
			{
				response = await _httpClient.PostAsync("createCheck", content);
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Ошибка соединения с Crypto Pay API при создании чека: {ex.Message}");
				return null;
			}

			var body = await response.Content.ReadAsStringAsync();
			Console.WriteLine($"CryptoPay createCheck status {(int)response.StatusCode}: {body}");

			if (!response.IsSuccessStatusCode)
			{
				Console.WriteLine($"Ошибка создания чека: {body}");
				return null;
			}

			try
			{
				var obj = JObject.Parse(body);
				var result = obj["result"];
				
				return new CheckInfo
				{
					CheckId = result?["check_id"]?.Value<long>() ?? 0,
					Hash = result?["hash"]?.ToString() ?? string.Empty,
					BotCheckUrl = result?["bot_check_url"]?.ToString() ?? string.Empty,
					Status = result?["status"]?.ToString() ?? string.Empty,
					CreatedAt = result?["created_at"]?.ToString() ?? string.Empty
				};
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Ошибка парсинга ответа создания чека: {ex.Message}");
				return null;
			}
		}

		// Получение баланса
		public async Task<decimal> GetBalanceAsync(string asset = "USDT")
		{
			try
			{
				var response = await _httpClient.GetAsync("getBalance");
				var body = await response.Content.ReadAsStringAsync();
				Console.WriteLine($"CryptoPay getBalance status {(int)response.StatusCode}: {body}");
				
				if (!response.IsSuccessStatusCode) return 0;

				var obj = JObject.Parse(body);
				var result = obj["result"];
				
				if (result is JArray balances)
				{
					foreach (var balance in balances)
					{
						if (balance["currency_code"]?.ToString() == asset)
						{
							return decimal.Parse(balance["available"]?.ToString() ?? "0", CultureInfo.InvariantCulture);
						}
					}
				}
				
				return 0;
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Ошибка получения баланса: {ex.Message}");
				return 0;
			}
		}
	}
} 