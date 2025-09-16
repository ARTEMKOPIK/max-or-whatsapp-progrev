using System.Net.Http;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace MaxTelegramBot
{
    public class User
    {
        [JsonProperty("id")]
        public long Id { get; set; }
        
        [JsonProperty("username")]
        public string Username { get; set; } = string.Empty;
        
        [JsonProperty("paid_accounts")]
        public int PaidAccounts { get; set; } = 0;
        
        [JsonProperty("referrals")]
        public int Referrals { get; set; } = 0;
        
        [JsonProperty("registration_date")]
        public DateTime RegistrationDate { get; set; } = DateTime.Now;
        
        [JsonProperty("referrer_id")]
        public long? ReferrerId { get; set; }
        
        [JsonProperty("phone_numbers")]
        public List<string> PhoneNumbers { get; set; } = new List<string>();
        
        [JsonProperty("affiliate_balance")]
        public decimal AffiliateBalance { get; set; } = 0;
        
        [JsonProperty("total_earned")]
        public decimal TotalEarned { get; set; } = 0;
        
        [JsonProperty("affiliate_code")]
        public string AffiliateCode { get; set; } = string.Empty;
    }

    public class Payment
    {
        [JsonProperty("id")]
        public long Id { get; set; }
        
        [JsonProperty("user_id")]
        public long UserId { get; set; }
        
        [JsonProperty("hash")]
        public string Hash { get; set; } = string.Empty;
        
        [JsonProperty("quantity")]
        public int Quantity { get; set; }
        
        [JsonProperty("amount_usdt")]
        public decimal AmountUsdt { get; set; }
        
        [JsonProperty("status")]
        public string Status { get; set; } = "pending"; // pending, paid, canceled
        
        [JsonProperty("created_at")]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        
        [JsonProperty("chat_id")]
        public long? ChatId { get; set; }
        
        [JsonProperty("message_id")]
        public int? MessageId { get; set; }
    }

    public class TimePayment
    {
        [JsonProperty("id")]
        public long Id { get; set; }

        [JsonProperty("user_id")]
        public long UserId { get; set; }

        [JsonProperty("phone_number")]
        public string PhoneNumber { get; set; } = string.Empty;

        [JsonProperty("hash")]
        public string Hash { get; set; } = string.Empty;

        [JsonProperty("hours")]
        public int Hours { get; set; }

        [JsonProperty("created_at")]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        [JsonProperty("chat_id")]
        public long? ChatId { get; set; }

        [JsonProperty("message_id")]
        public int? MessageId { get; set; }
    }

    public class ReferralEarning
    {
        [JsonProperty("id")]
        public long Id { get; set; }
        
        [JsonProperty("referrer_id")]
        public long ReferrerId { get; set; }
        
        [JsonProperty("referred_user_id")]
        public long ReferredUserId { get; set; }
        
        [JsonProperty("amount_usdt")]
        public decimal AmountUsdt { get; set; }
        
        [JsonProperty("earning_type")]
        public string EarningType { get; set; } = "registration"; // registration, payment
        
        [JsonProperty("status")]
        public string Status { get; set; } = "pending"; // pending, paid, canceled
        
        [JsonProperty("created_at")]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        
        [JsonProperty("description")]
        public string Description { get; set; } = string.Empty;
    }

    public class WithdrawalRequest
    {
        [JsonProperty("id")]
        public long Id { get; set; }
        
        [JsonProperty("user_id")]
        public long UserId { get; set; }
        
        [JsonProperty("amount_usdt")]
        public decimal AmountUsdt { get; set; }
        
        [JsonProperty("wallet_address")]
        public string WalletAddress { get; set; } = string.Empty;
        
        [JsonProperty("network")]
        public string Network { get; set; } = "TRC20"; // TRC20, ERC20, BEP20
        
        [JsonProperty("status")]
        public string Status { get; set; } = "pending"; // pending, processing, completed, rejected
        
        [JsonProperty("created_at")]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        
        [JsonProperty("processed_at")]
        public DateTime? ProcessedAt { get; set; }
        
        [JsonProperty("admin_notes")]
        public string? AdminNotes { get; set; }
    }

    public class AffiliateUser
    {
        [JsonProperty("id")]
        public long Id { get; set; }
        
        [JsonProperty("user_id")]
        public long UserId { get; set; }
        
        [JsonProperty("affiliate_code")]
        public string AffiliateCode { get; set; } = string.Empty;
        
        [JsonProperty("affiliate_balance")]
        public decimal AffiliateBalance { get; set; } = 0;
        
        [JsonProperty("total_earned")]
        public decimal TotalEarned { get; set; } = 0;
        
        [JsonProperty("total_referrals")]
        public int TotalReferrals { get; set; } = 0;
        
        [JsonProperty("active_referrals")]
        public int ActiveReferrals { get; set; } = 0;
        
        [JsonProperty("created_at")]
        public DateTime CreatedAt { get; set; } = DateTime.Now;
        
        [JsonProperty("updated_at")]
        public DateTime UpdatedAt { get; set; } = DateTime.Now;
    }

    public class SupabaseService
    {
        private readonly HttpClient _httpClient;
        private readonly string _supabaseUrl;
        private readonly string _supabaseKey;

        // Публичные свойства для доступа к приватным полям
        public HttpClient HttpClient => _httpClient;
        public string SupabaseUrl => _supabaseUrl;

        public SupabaseService(string supabaseUrl, string supabaseKey)
        {
            _supabaseUrl = supabaseUrl;
            _supabaseKey = supabaseKey;
            
            _httpClient = new HttpClient();
            _httpClient.DefaultRequestHeaders.Add("apikey", _supabaseKey);
            _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {_supabaseKey}");
        }

        // Получение пользователя по ID
        public async Task<User?> GetUserAsync(long userId)
        {
            try
            {
                var response = await _httpClient.GetAsync($"{_supabaseUrl}/rest/v1/users?id=eq.{userId}");
                if (response.IsSuccessStatusCode)
                {
                    var json = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"GetUserAsync для {userId}: {response.StatusCode} - {json}");
                    
                    var users = JsonConvert.DeserializeObject<List<User>>(json);
                    if (users != null && users.Count > 0)
                    {
                        var user = users[0];
                        Console.WriteLine($"Найден пользователь: ID={user.Id}, Username={user.Username}, RegistrationDate={user.RegistrationDate}");
                        
                        // Устанавливаем значения по умолчанию для отсутствующих колонок
                        if (string.IsNullOrEmpty(user.AffiliateCode))
                        {
                            user.AffiliateCode = $"REF{userId}";
                        }
                        
                        return user;
                    }
                }
                else
                {
                    Console.WriteLine($"GetUserAsync для {userId}: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка GetUserAsync для {userId}: {ex.Message}");
            }
            return null;
        }

        public async Task<User> CreateUserAsync(long userId, string username, long? referrerId = null)
        {
            try
            {
                var user = new
                {
                    id = userId,
                    username = username,
                    paid_accounts = 0,
                    referrals = 0,
                    registration_date = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                    referrer_id = referrerId
                };

                var json = JsonConvert.SerializeObject(user);
                Console.WriteLine($"Отправляем JSON в Supabase: {json}");
                
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                
                var response = await _httpClient.PostAsync($"{_supabaseUrl}/rest/v1/users", content);
                var responseContent = await response.Content.ReadAsStringAsync();
                
                Console.WriteLine($"Ответ Supabase: {response.StatusCode} - {responseContent}");

                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"Пользователь {userId} успешно создан в базе данных");
                    
                    // Если есть реферер, увеличиваем его счетчик рефералов
                    if (referrerId.HasValue)
                    {
                        await IncrementReferralsAsync(referrerId.Value);
                    }
                }
                else
                {
                    Console.WriteLine($"Ошибка создания пользователя: {response.StatusCode} - {responseContent}");
                }

                return new User
                {
                    Id = userId,
                    Username = username,
                    RegistrationDate = DateTime.Now,
                    ReferrerId = referrerId
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Исключение при создании пользователя {userId}: {ex.Message}");
                return new User
                {
                    Id = userId,
                    Username = username,
                    RegistrationDate = DateTime.Now,
                    ReferrerId = referrerId
                };
            }
        }

        public async Task<bool> AddPaidAccountsAsync(long userId, int accountsToAdd)
        {
            try
            {
                Console.WriteLine($"AddPaidAccountsAsync: Прибавляю пользователю {userId} еще {accountsToAdd} аккаунтов");
                
                // Сначала получаем текущее количество аккаунтов
                var currentUser = await GetUserAsync(userId);
                if (currentUser == null)
                {
                    Console.WriteLine($"Пользователь {userId} не найден");
                    return false;
                }
                
                var newTotal = currentUser.PaidAccounts + accountsToAdd;
                Console.WriteLine($"Текущее количество: {currentUser.PaidAccounts}, прибавляем: {accountsToAdd}, итого: {newTotal}");
                
                var updateData = new { paid_accounts = newTotal };
                var json = JsonConvert.SerializeObject(updateData);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                
                Console.WriteLine($"Отправляю PATCH запрос: {json}");
                
                var response = await _httpClient.PatchAsync($"{_supabaseUrl}/rest/v1/users?id=eq.{userId}", content);
                if (response.IsSuccessStatusCode)
                {
                    var responseContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"AddPaidAccountsAsync для {userId}: {response.StatusCode} - {responseContent}");
                    
                    // Автоматически начисляем комиссию рефереру если есть
                    if (currentUser.ReferrerId.HasValue)
                    {
                        await PayReferralCommissionAsync(currentUser.ReferrerId.Value, userId, accountsToAdd);
                    }
                    return true;
                }
                else
                {
                    Console.WriteLine($"Ошибка обновления: {response.StatusCode} - {await response.Content.ReadAsStringAsync()}");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Исключение при прибавлении оплаченных аккаунтов для {userId}: {ex.Message}");
                return false;
            }
        }

        public async Task<bool> UpdatePaidAccountsAsync(long userId, int paidAccounts)
        {
            try
            {
                Console.WriteLine($"UpdatePaidAccountsAsync: Устанавливаю пользователю {userId} точно {paidAccounts} аккаунтов");
                
                var updateData = new { paid_accounts = paidAccounts };
                var json = JsonConvert.SerializeObject(updateData);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                
                Console.WriteLine($"Отправляю PATCH запрос: {json}");
                
                var response = await _httpClient.PatchAsync($"{_supabaseUrl}/rest/v1/users?id=eq.{userId}", content);
                var responseContent = await response.Content.ReadAsStringAsync();
                
                Console.WriteLine($"Ответ PATCH: {response.StatusCode} - {responseContent}");

                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"Успешно установлено пользователю {userId}: {paidAccounts} аккаунтов");
                    return true;
                }
                else
                {
                    Console.WriteLine($"Ошибка обновления: {response.StatusCode} - {responseContent}");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Исключение при обновлении оплаченных аккаунтов для {userId}: {ex.Message}");
                return false;
            }
        }

        public async Task<bool> IncrementReferralsAsync(long userId)
        {
            try
            {
                var user = await GetUserAsync(userId);
                if (user == null) return false;

                user.Referrals++;
                var updateData = new { referrals = user.Referrals };

                var json = JsonConvert.SerializeObject(updateData);
                var content = new StringContent(json, Encoding.UTF8, "application/json");

                var response = await _httpClient.PatchAsync($"{_supabaseUrl}/rest/v1/users?id=eq.{userId}", content);
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка увеличения рефералов для {userId}: {ex.Message}");
                return false;
            }
        }

        // Партнерская программа - получение рефералов пользователя
        public async Task<List<User>> GetUserReferralsAsync(long userId)
        {
            try
            {
                var response = await _httpClient.GetAsync($"{_supabaseUrl}/rest/v1/users?referrer_id=eq.{userId}");
                if (response.IsSuccessStatusCode)
                {
                    var json = await response.Content.ReadAsStringAsync();
                    var users = JsonConvert.DeserializeObject<List<User>>(json);
                    return users ?? new List<User>();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка получения рефералов для {userId}: {ex.Message}");
            }
            return new List<User>();
        }

        // Партнерская программа - получение заработка пользователя
        public async Task<List<ReferralEarning>> GetUserEarningsAsync(long userId)
        {
            try
            {
                var response = await _httpClient.GetAsync($"{_supabaseUrl}/rest/v1/referral_earnings?referrer_id=eq.{userId}");
                if (response.IsSuccessStatusCode)
                {
                    var json = await response.Content.ReadAsStringAsync();
                    var earnings = JsonConvert.DeserializeObject<List<ReferralEarning>>(json);
                    return earnings ?? new List<ReferralEarning>();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка получения заработка для {userId}: {ex.Message}");
            }
            return new List<ReferralEarning>();
        }

        // Партнерская программа - создание записи о заработке
        public async Task<bool> CreateReferralEarningAsync(long referrerId, long referredUserId, decimal amountUsdt, string earningType = "payment", string description = "")
        {
            try
            {
                var earning = new ReferralEarning
                {
                    ReferrerId = referrerId,
                    ReferredUserId = referredUserId,
                    AmountUsdt = amountUsdt,
                    EarningType = earningType,
                    Status = "pending",
                    Description = description
                };
                
                var json = JsonConvert.SerializeObject(earning);
                var content = new StringContent(json, Encoding.UTF8, "application/json");

                var response = await _httpClient.PostAsync($"{_supabaseUrl}/rest/v1/referral_earnings", content);
                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[AFFILIATE] ✅ Создана запись о заработке: {referrerId} получил {amountUsdt} USDT от {referredUserId}");
                    return true;
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"[AFFILIATE] ❌ Ошибка создания записи о заработке: {response.StatusCode} - {errorContent}");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка создания записи о заработке: {ex.Message}");
                return false;
            }
        }

        // Партнерская программа - обновление баланса пользователя
        public async Task<bool> UpdateUserAffiliateBalanceAsync(long userId, decimal newBalance, decimal newTotalEarned)
        {
            try
            {
                var updateData = new 
                { 
                    affiliate_balance = newBalance,
                    total_earned = newTotalEarned
                };
                
                var json = JsonConvert.SerializeObject(updateData);
                var content = new StringContent(json, Encoding.UTF8, "application/json");

                var response = await _httpClient.PatchAsync($"{_supabaseUrl}/rest/v1/affiliate_users?user_id=eq.{userId}", content);
                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[AFFILIATE] ✅ Обновлен баланс пользователя {userId}: баланс={newBalance}, всего заработано={newTotalEarned}");
                    return true;
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"[AFFILIATE] ❌ Ошибка обновления баланса {userId}: {response.StatusCode} - {errorContent}");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка обновления баланса {userId}: {ex.Message}");
                return false;
            }
        }

        // Партнерская программа - создание записи о выводе средств
        public async Task<bool> CreateWithdrawalRequestAsync(long userId, decimal amount, string walletAddress, string network)
        {
            try
            {
                // Для мгновенных выплат через Crypto Pay сразу отмечаем запрос как выполненный
                var now = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");

                var payload = new
                {
                    user_id = userId,
                    amount_usdt = amount,
                    wallet_address = walletAddress,
                    network = network,
                    status = "completed",
                    created_at = now,
                    processed_at = now
                };

                var json = JsonConvert.SerializeObject(payload);
                var content = new StringContent(json, Encoding.UTF8, "application/json");

                var response = await _httpClient.PostAsync($"{_supabaseUrl}/rest/v1/withdrawal_requests", content);
                var resp = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"CreateWithdrawalRequestAsync: {response.StatusCode} - {resp}");
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка создания запроса на вывод: {ex.Message}");
                return false;
            }
        }

        // Партнерская программа - получение запросов на вывод пользователя
        public async Task<List<WithdrawalRequest>> GetUserWithdrawalsAsync(long userId)
        {
            try
            {
                var response = await _httpClient.GetAsync($"{_supabaseUrl}/rest/v1/withdrawal_requests?user_id=eq.{userId}&select=*&order=created_at.desc");
                var content = await response.Content.ReadAsStringAsync();
                
                if (response.IsSuccessStatusCode)
                {
                    var settings = new JsonSerializerSettings
                    {
                        DateFormatHandling = DateFormatHandling.IsoDateFormat,
                        DateTimeZoneHandling = DateTimeZoneHandling.Utc
                    };
                    
                    var withdrawals = JsonConvert.DeserializeObject<List<WithdrawalRequest>>(content, settings);
                    return withdrawals ?? new List<WithdrawalRequest>();
                }
                return new List<WithdrawalRequest>();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка получения выводов для {userId}: {ex.Message}");
                return new List<WithdrawalRequest>();
            }
        }

        // Партнерская программа - генерация уникального реферального кода
        public async Task<string> GenerateAffiliateCodeAsync(long userId)
        {
            try
            {
                // Проверяем есть ли уже запись в affiliate_users
                var existingUser = await GetAffiliateUserAsync(userId);
                if (existingUser != null && !string.IsNullOrEmpty(existingUser.AffiliateCode))
                {
                    Console.WriteLine($"[AFFILIATE] ✅ Найден существующий код для {userId}: {existingUser.AffiliateCode}");
                    return existingUser.AffiliateCode;
                }

                // Генерируем уникальный код
                var timestamp = DateTime.UtcNow.ToString("yyyyMMdd");
                var random = new Random().Next(1000, 9999);
                var code = $"REF{userId}{timestamp}{random}";
                
                // Создаем запись в affiliate_users
                var affiliateUser = new AffiliateUser
                {
                    UserId = userId,
                    AffiliateCode = code,
                    AffiliateBalance = 0,
                    TotalEarned = 0,
                    TotalReferrals = 0,
                    ActiveReferrals = 0
                };
                
                var json = JsonConvert.SerializeObject(affiliateUser);
                var content = new StringContent(json, Encoding.UTF8, "application/json");

                var response = await _httpClient.PostAsync($"{_supabaseUrl}/rest/v1/affiliate_users", content);
                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[AFFILIATE] ✅ Создан реферальный код для {userId}: {code}");
                    return code;
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"[AFFILIATE] ❌ Ошибка создания записи для {userId}: {response.StatusCode} - {errorContent}");
                    
                    // Возвращаем временный код если не удалось создать запись
                    return code;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка генерации реферального кода для {userId}: {ex.Message}");
                return $"REF{userId}";
            }
        }

        // Получение affiliate пользователя
        public async Task<AffiliateUser?> GetAffiliateUserAsync(long userId)
        {
            try
            {
                var response = await _httpClient.GetAsync($"{_supabaseUrl}/rest/v1/affiliate_users?user_id=eq.{userId}");
                if (response.IsSuccessStatusCode)
                {
                    var json = await response.Content.ReadAsStringAsync();
                    var users = JsonConvert.DeserializeObject<List<AffiliateUser>>(json);
                    return users?.FirstOrDefault();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка получения affiliate пользователя {userId}: {ex.Message}");
            }
            return null;
        }

        // Партнерская программа - поиск пользователя по реферальному коду
        public async Task<User?> GetUserByAffiliateCodeAsync(string affiliateCode)
        {
            try
            {
                var response = await _httpClient.GetAsync($"{_supabaseUrl}/rest/v1/affiliate_users?affiliate_code=eq.{affiliateCode}");
                if (response.IsSuccessStatusCode)
                {
                    var json = await response.Content.ReadAsStringAsync();
                    var affiliateUsers = JsonConvert.DeserializeObject<List<AffiliateUser>>(json);
                    var affiliateUser = affiliateUsers?.FirstOrDefault();
                    
                    if (affiliateUser != null)
                    {
                        return await GetUserAsync(affiliateUser.UserId);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка поиска пользователя по коду {affiliateCode}: {ex.Message}");
            }
            return null;
        }

        // Партнерская программа - выплата комиссии рефералу
        private async Task PayReferralCommissionAsync(long referrerId, long referredUserId, int accountsPurchased)
        {
            try
            {
                // Получаем данные реферера
                var referrer = await GetUserAsync(referrerId);
                var referredUser = await GetUserAsync(referredUserId);
                
                if (referrer == null || referredUser == null)
                {
                    Console.WriteLine($"[AFFILIATE] ❌ Не удалось найти пользователей: referrer={referrerId}, referred={referredUserId}");
                    return;
                }

                // Рассчитываем комиссию (10% от стоимости аккаунтов)
                var accountsCost = accountsPurchased * 2.00m; // 2 USDT за аккаунт
                var commission = accountsCost * 0.10m; // 10% комиссия

                // Создаем запись о заработке
                var earningCreated = await CreateReferralEarningAsync(
                    referrerId, 
                    referredUserId, 
                    commission, 
                    "payment", 
                    $"Комиссия с покупки {accountsPurchased} аккаунтов пользователем {referredUser.Username}"
                );

                if (earningCreated)
                {
                    // Получаем текущий affiliate пользователя
                    var affiliateUser = await GetAffiliateUserAsync(referrerId);
                    if (affiliateUser == null)
                    {
                        // Создаем запись если её нет
                        await GenerateAffiliateCodeAsync(referrerId);
                        affiliateUser = await GetAffiliateUserAsync(referrerId);
                    }

                    if (affiliateUser != null)
                    {
                        // Обновляем баланс
                        var newBalance = affiliateUser.AffiliateBalance + commission;
                        var newTotalEarned = affiliateUser.TotalEarned + commission;
                        
                        await UpdateUserAffiliateBalanceAsync(referrerId, newBalance, newTotalEarned);
                        
                        Console.WriteLine($"[AFFILIATE] ✅ Выплачена комиссия {referrer.Username}: +{commission:F2} USDT (баланс: {newBalance:F2})");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка выплаты комиссии: {ex.Message}");
            }
        }

        public async Task<(bool success, string message)> AddPhoneNumberAsync(long userId, string phoneNumber)
        {
            try
            {
                var normalizedPhone = NormalizePhone(phoneNumber);
                if (string.IsNullOrEmpty(normalizedPhone) || normalizedPhone.Length != 11)
                    return (false, "❌ Неверный формат номера");

                var user = await GetUserAsync(userId);
                if (user != null)
                {
                    // Нормализуем уже существующие номера для единообразия
                    user.PhoneNumbers = user.PhoneNumbers.Select(NormalizePhone).Distinct().ToList();

                    if (!user.PhoneNumbers.Contains(normalizedPhone))
                    {
                        user.PhoneNumbers.Add(normalizedPhone);
                        var updateData = new { phone_numbers = user.PhoneNumbers.ToArray() };
                        var json = JsonConvert.SerializeObject(updateData);
                        var content = new StringContent(json, Encoding.UTF8, "application/json");

                        var response = await _httpClient.PatchAsync($"{_supabaseUrl}/rest/v1/users?id=eq.{userId}", content);
                        if (response.IsSuccessStatusCode)
                        {
                            return (true, $"✅ Номер {normalizedPhone} успешно добавлен в ваши аккаунты");
                        }
                        else
                        {
                            return (false, $"❌ Ошибка при сохранении номера {normalizedPhone}");
                        }
                    }
                    return (false, $"⚠️ Номер {normalizedPhone} уже есть в ваших аккаунтах");
                }
                return (false, "❌ Ошибка загрузки данных пользователя");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка добавления номера телефона для {userId}: {ex.Message}");
                return (false, $"❌ Ошибка: {ex.Message}");
            }
        }

        public async Task<bool> RemovePhoneNumberAsync(long userId, string phoneNumber)
        {
            try
            {
                var user = await GetUserAsync(userId);
                if (user != null)
                {
                    user.PhoneNumbers.Remove(phoneNumber);
                    var updateData = new { phone_numbers = user.PhoneNumbers.ToArray() };
                    var json = JsonConvert.SerializeObject(updateData);
                    var content = new StringContent(json, Encoding.UTF8, "application/json");
                    
                    var response = await _httpClient.PatchAsync($"{_supabaseUrl}/rest/v1/users?id=eq.{userId}", content);
                    return response.IsSuccessStatusCode;
                }
                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка удаления номера телефона для {userId}: {ex.Message}");
                return false;
            }
        }

        public async Task<User> GetOrCreateUserAsync(long userId, string username, long? referrerId = null)
        {
            try
            {
                var existingUser = await GetUserAsync(userId);
                if (existingUser != null)
                {
                    Console.WriteLine($"Пользователь {userId} уже существует в базе, возвращаем существующего");
                    return existingUser;
                }

                Console.WriteLine($"Пользователь {userId} не найден, создаем нового");
                return await CreateUserAsync(userId, username, referrerId);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка в GetOrCreateUserAsync: {ex.Message}");
                // Возвращаем локальный объект если база недоступна
                return new User
                {
                    Id = userId,
                    Username = username,
                    RegistrationDate = DateTime.Now,
                    ReferrerId = referrerId
                };
            }
        }

        public async Task<bool> CreatePaymentAsync(long userId, string hash, int quantity, decimal amountUsdt, long chatId, int messageId)
        {
            try
            {
                var payload = new
                {
                    user_id = userId,
                    hash = hash,
                    quantity = quantity,
                    amount_usdt = amountUsdt,
                    status = "pending",
                    created_at = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                    chat_id = chatId,
                    message_id = messageId
                };
                var json = JsonConvert.SerializeObject(payload);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var response = await _httpClient.PostAsync($"{_supabaseUrl}/rest/v1/payments", content);
                var resp = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"CreatePaymentAsync: {response.StatusCode} - {resp}");
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка CreatePaymentAsync: {ex.Message}");
                return false;
            }
        }

        public async Task<bool> CreateTimePaymentAsync(long userId, string phone, int hours, string hash, long chatId, int messageId)
        {
            try
            {
                var payload = new
                {
                    user_id = userId,
                    phone_number = phone,
                    hash = hash,
                    hours = hours,
                    created_at = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                    chat_id = chatId,
                    message_id = messageId
                };
                var json = JsonConvert.SerializeObject(payload);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var response = await _httpClient.PostAsync($"{_supabaseUrl}/rest/v1/time_payments", content);
                var resp = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"CreateTimePaymentAsync: {response.StatusCode} - {resp}");
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка CreateTimePaymentAsync: {ex.Message}");
                return false;
            }
        }

        public async Task<bool> DeleteTimePaymentByHashAsync(string hash)
        {
            try
            {
                var response = await _httpClient.DeleteAsync($"{_supabaseUrl}/rest/v1/time_payments?hash=eq.{hash}");
                var resp = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"DeleteTimePaymentByHashAsync: {response.StatusCode} - {resp}");
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка DeleteTimePaymentByHashAsync: {ex.Message}");
                return false;
            }
        }

        public async Task<Payment?> GetPaymentByHashAsync(string hash)
        {
            try
            {
                var response = await _httpClient.GetAsync($"{_supabaseUrl}/rest/v1/payments?hash=eq.{hash}&select=*");
                var content = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"GetPaymentByHashAsync: {response.StatusCode} - {content}");
                if (!response.IsSuccessStatusCode) return null;
                var list = JsonConvert.DeserializeObject<List<Payment>>(content);
                return list?.FirstOrDefault();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка GetPaymentByHashAsync: {ex.Message}");
                return null;
            }
        }

        public async Task<bool> MarkPaymentPaidAsync(string hash)
        {
            try
            {
                var payload = new { status = "paid" };
                var json = JsonConvert.SerializeObject(payload);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var response = await _httpClient.PatchAsync($"{_supabaseUrl}/rest/v1/payments?hash=eq.{hash}", content);
                var resp = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"MarkPaymentPaidAsync: {response.StatusCode} - {resp}");
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка MarkPaymentPaidAsync: {ex.Message}");
                return false;
            }
        }

        public async Task<bool> MarkPaymentCanceledAsync(string hash)
        {
            try
            {
                var payload = new { status = "canceled" };
                var json = JsonConvert.SerializeObject(payload);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var response = await _httpClient.PatchAsync($"{_supabaseUrl}/rest/v1/payments?hash=eq.{hash}", content);
                var resp = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"MarkPaymentCanceledAsync: {response.StatusCode} - {resp}");
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка MarkPaymentCanceledAsync: {ex.Message}");
                return false;
            }
        }

        public async Task<List<User>> GetAllUsersAsync()
        {
            try
            {
                var response = await _httpClient.GetAsync($"{_supabaseUrl}/rest/v1/users?select=id,username,paid_accounts,phone_numbers");
                var content = await response.Content.ReadAsStringAsync();
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"GetAllUsersAsync: {response.StatusCode} - {content}");
                    return new List<User>();
                }

                var users = JsonConvert.DeserializeObject<List<User>>(content);
                return users ?? new List<User>();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка GetAllUsersAsync: {ex.Message}");
                return new List<User>();
            }
        }

        public async Task<List<long>> GetAllUserIdsAsync()
        {
            var userIds = new List<long>();
            try
            {
                // Берем только id для минимизации нагрузки
                var response = await _httpClient.GetAsync($"{_supabaseUrl}/rest/v1/users?select=id");
                var content = await response.Content.ReadAsStringAsync();
                if (!response.IsSuccessStatusCode)
                {
                    Console.WriteLine($"[BROADCAST] Ошибка получения списка пользователей: {response.StatusCode} - {content}");
                    return userIds;
                }

                try
                {
                    var users = JsonConvert.DeserializeObject<List<User>>(content);
                    if (users != null)
                    {
                        foreach (var u in users)
                        {
                            if (u.Id != 0)
                            {
                                userIds.Add(u.Id);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // На случай, если десериализация в User не сработает из-за select=id, пробуем разобрать вручную
                    Console.WriteLine($"[BROADCAST] Предупреждение десериализации users: {ex.Message}");
                    var arr = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(content) ?? new List<Dictionary<string, object>>();
                    foreach (var dict in arr)
                    {
                        if (dict.TryGetValue("id", out var val))
                        {
                            if (val is long l) userIds.Add(l);
                            else if (val is int i) userIds.Add(i);
                            else if (long.TryParse(val?.ToString(), out var p)) userIds.Add(p);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BROADCAST] Исключение при получении списка пользователей: {ex.Message}");
            }
            return userIds.Distinct().ToList();
        }

        public async Task<int> GetPaidAccountsAsync(long userId)
        {
            var user = await GetUserAsync(userId);
            return user?.PaidAccounts ?? 0;
        }

        public async Task<bool> TryConsumeOnePaidAccountAsync(long userId)
        {
            try
            {
                var user = await GetUserAsync(userId);
                if (user == null) return false;
                if (user.PaidAccounts <= 0) return false;
                var newTotal = user.PaidAccounts - 1;
                var updateData = new { paid_accounts = newTotal };
                var json = JsonConvert.SerializeObject(updateData);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var response = await _httpClient.PatchAsync($"{_supabaseUrl}/rest/v1/users?id=eq.{userId}", content);
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка списания оплаченного аккаунта: {ex.Message}");
                return false;
            }
        }

        public async Task<bool> DecreasePaidAccountsAsync(long userId, int accountsToRemove)
        {
            try
            {
                var user = await GetUserAsync(userId);
                if (user != null)
                {
                    var newCount = Math.Max(0, user.PaidAccounts - accountsToRemove);
                    var updateData = new { paid_accounts = newCount };
                    var json = JsonConvert.SerializeObject(updateData);
                    var content = new StringContent(json, Encoding.UTF8, "application/json");
                    
                    var response = await _httpClient.PatchAsync($"{_supabaseUrl}/rest/v1/users?id=eq.{userId}", content);
                    return response.IsSuccessStatusCode;
                }
                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка уменьшения оплаченных аккаунтов для {userId}: {ex.Message}");
                return false;
            }
        }

        public async Task<string?> GetRandomPhoneNumberAsync(long excludeUserId, List<string> excludeNumbers)
        {
            try
            {
                Console.WriteLine($"[SUPABASE] GetRandomPhoneNumberAsync: excludeUserId={excludeUserId}, excludeNumbers.Count={excludeNumbers.Count}");
                
                // Получаем ВСЕ номера из таблицы active_numbers
                var url = $"{_supabaseUrl}/rest/v1/active_numbers?select=phone";
                Console.WriteLine($"[SUPABASE] Запрос к: {url}");
                
                var response = await _httpClient.GetAsync(url);
                Console.WriteLine($"[SUPABASE] Ответ: {response.StatusCode}");
                
                if (response.IsSuccessStatusCode)
                {
                    var responseContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"[SUPABASE] Содержимое ответа: {responseContent}");
                    
                    var activeNumbers = JsonConvert.DeserializeObject<List<JObject>>(responseContent);
                    Console.WriteLine($"[SUPABASE] Активных номеров найдено: {activeNumbers?.Count ?? 0}");
                    
                    if (activeNumbers != null && activeNumbers.Any())
                    {
                        var allNumbers = new List<string>();
                        
                        foreach (var activeNumber in activeNumbers)
                        {
                            if (activeNumber["phone"] != null)
                            {
                                var phone = activeNumber["phone"].ToString();
                                if (!string.IsNullOrEmpty(phone))
                                {
                                    allNumbers.Add(phone);
                                    Console.WriteLine($"[SUPABASE] Добавлен активный номер: {phone}");
                                }
                            }
                        }
                        
                        Console.WriteLine($"[SUPABASE] Всего номеров найдено: {allNumbers.Count}");
                        
                        // Исключаем только конкретные номера, которые уже использовались
                        var availableNumbers = allNumbers.Where(num => !excludeNumbers.Contains(num)).ToList();
                        Console.WriteLine($"[SUPABASE] Доступных номеров после исключения: {availableNumbers.Count}");
                        Console.WriteLine($"[SUPABASE] Исключенные номера: {string.Join(", ", excludeNumbers)}");
                        
                        if (availableNumbers.Any())
                        {
                            // Выбираем случайный номер
                            var random = new Random();
                            var randomIndex = random.Next(availableNumbers.Count);
                            var selectedNumber = availableNumbers[randomIndex];
                            Console.WriteLine($"[SUPABASE] Выбран случайный номер: {selectedNumber} (индекс: {randomIndex})");
                            return selectedNumber;
                        }
                        else
                        {
                            Console.WriteLine("[SUPABASE] Нет доступных номеров для выбора");
                        }
                    }
                    else
                    {
                        Console.WriteLine("[SUPABASE] Нет активных номеров в базе");
                    }
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    Console.WriteLine($"[SUPABASE] Ошибка HTTP: {response.StatusCode} - {errorContent}");
                }
                
                Console.WriteLine("[SUPABASE] Возвращаю null");
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SUPABASE] Ошибка получения случайного номера: {ex.Message}");
                return null;
            }
        }

        public static string NormalizePhone(string rawPhone)
        {
            try
            {
                var digits = new string((rawPhone ?? string.Empty).Where(char.IsDigit).ToArray());
                if (string.IsNullOrEmpty(digits))
                    return string.Empty;

                if (digits.Length > 11)
                    digits = digits.Substring(digits.Length - 11);

                if (digits.Length == 11)
                {
                    if (digits.StartsWith("8"))
                        digits = "7" + digits.Substring(1);
                    else if (!digits.StartsWith("7"))
                        digits = "7" + digits.Substring(digits.Length - 10);
                    return digits;
                }

                if (digits.Length == 10)
                    return "7" + digits;

                return digits;
            }
            catch
            {
                return rawPhone ?? string.Empty;
            }
        }

        public static string NormalizePhoneForActive(string rawPhone)
        {
            try
            {
                var normalized = NormalizePhone(rawPhone);
                if (string.IsNullOrEmpty(normalized))
                    return string.Empty;
                return normalized.Length > 1 ? normalized.Substring(1) : normalized;
            }
            catch { return rawPhone ?? string.Empty; }
        }

        public async Task<bool> InsertActiveNumberAsync(long userId, string phoneNormalized, DateTime endsAtUtc)
        {
            try
            {
                var payload = new
                {
                    user_id = userId,
                    phone = phoneNormalized,
                    started_at = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"),
                    ends_at = endsAtUtc.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
                };
                var json = JsonConvert.SerializeObject(payload);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var response = await _httpClient.PostAsync($"{_supabaseUrl}/rest/v1/active_numbers", content);
                var resp = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"InsertActiveNumberAsync: {response.StatusCode} - {resp}");
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка InsertActiveNumberAsync: {ex.Message}");
                return false;
            }
        }

        public async Task<bool> DeleteActiveNumberByPhoneAsync(string phoneNormalized)
        {
            try
            {
                var response = await _httpClient.DeleteAsync($"{_supabaseUrl}/rest/v1/active_numbers?phone=eq.{phoneNormalized}");
                var resp = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"DeleteActiveNumberByPhoneAsync: {response.StatusCode} - {resp}");
                return response.IsSuccessStatusCode;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка DeleteActiveNumberByPhoneAsync: {ex.Message}");
                return false;
            }
        }
    }
} 