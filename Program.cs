using Telegram.Bot;
using Telegram.Bot.Exceptions;
using Telegram.Bot.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using Telegram.Bot.Types.ReplyMarkups;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json.Linq;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace MaxTelegramBot
{
    class Program
    {
        private static ITelegramBotClient _botClient;
        private static string _botToken = "8151467364:AAHavK2OpIuO2ZQt8crnoupXAYLFDfspNc0"; // Токен бота
        private static SupabaseService _supabaseService;
        private static CryptoPayService _cryptoPayService;
        private const decimal PricePerAccountUsdt = 0.50m;
        private const decimal PricePerSixHoursUsdt = 0.50m;
        private static decimal CalculateHoursPrice(int hours) => (PricePerSixHoursUsdt / 6m) * hours;
        private static CancellationTokenSource _cts; // для управляемого выключения
        private static bool _isShuttingDown = false;
        private static bool _maintenance = false; // режим обслуживания

        // Базовый URL WhatsApp Web
        private const string WhatsAppWebUrl = "https://web.whatsapp.com/";
        
        // Данные Supabase
        private static string _supabaseUrl = "https://jlsmbiebfqqgncihdfki.supabase.co";
        private static string _supabaseKey = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impsc21iaWViZnFxZ25jaWhkZmtpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTU3MjUwODEsImV4cCI6MjA3MTMwMTA4MX0.MEuQR35kJ47OqGiP0eVx-gj33DlMqrlBT329foHEcYs";
        // Crypto Pay API токен (замените на ваш)
        private static string _cryptoPayToken = "362233:AAsMjUotcz8zmMsstcRKFiacIlsQ2p7JObA";
        
        // Партнерская программа - настройки
        private const decimal ReferralPaymentCommission = 0.10m; // Комиссия с платежей реферала (10%)
        private const decimal MinimumWithdrawal = 0.05m; // Минимальная сумма для вывода (USDT)
        private const decimal MaximumWithdrawal = 1000.00m; // Максимальная сумма для вывода (USDT)

        private static readonly Dictionary<long, string> _awaitingCodeSessionDirByUser = new();
        private static readonly Dictionary<long, string> _userPhoneNumbers = new(); // Номера телефонов по пользователям
        private static readonly Dictionary<long, string> _lastSessionDirByUser = new Dictionary<long, string>();
        private static readonly HashSet<long> _awaitingPaymentQtyUserIds = new HashSet<long>();
        private static readonly Dictionary<long, string> _awaitingHoursByUser = new();
        private static readonly Dictionary<long, string> _awaitingIntervalByUser = new();
        private static readonly Dictionary<string, string> _sessionDirByPhone = new Dictionary<string, string>();

        // Сессии для повторной проверки входа
        private static readonly Dictionary<long, string> _verificationSessionDirByUser = new Dictionary<long, string>();

        private static readonly Dictionary<string, DateTime> _warmingEndsByPhone = new Dictionary<string, DateTime>();
        private static readonly Dictionary<string, CancellationTokenSource> _warmingCtsByPhone = new Dictionary<string, CancellationTokenSource>();
        private static readonly Dictionary<string, TimeSpan> _warmingRemainingByPhone = new Dictionary<string, TimeSpan>();
        private static readonly Dictionary<string, (int Min, int Max)> _warmingIntervalsByPhone = new();
        // Тип прогрева для каждого номера: "Max" или "WhatsApp"
        private static readonly Dictionary<string, string> _warmingTypeByPhone = new();
        private static readonly Dictionary<long, string> _resumeFreeByUser = new Dictionary<long, string>();

        private static readonly string WarmingStateFile = "warming_state.json";
        private static readonly object _warmingStateLock = new();

        private class PersistedWarmingState
        {
            public Dictionary<string, double> Running { get; set; } = new();
            public Dictionary<string, double> Paused { get; set; } = new();
            public Dictionary<string, int[]>? Intervals { get; set; } = new();
        }

        // Отслеживание последнего использованного номера для каждого пользователя
        private static readonly Dictionary<long, string> _lastUsedNumberByUser = new Dictionary<long, string>();
        
        // Управление ресурсами для множественных браузеров
        private static readonly SemaphoreSlim _browserSemaphore = new SemaphoreSlim(30, 30); // Максимум 30 браузеров

        // Таймеры ожидания кода авторизации по пользователям
        private static readonly Dictionary<long, List<(string sessionDir, CancellationTokenSource cts)>> _authTimeoutCtsByUser = new();

        private enum BroadcastMode { None, Copy, Forward }
        private static BroadcastMode _awaitingBroadcastMode = BroadcastMode.None; // ожидание сообщения для рассылки
        private static bool _isBroadcastInProgress = false; // флаг активной рассылки

        // Состояние админ-панели для обработки ввода
        private static readonly Dictionary<long, string> _adminActionState = new Dictionary<long, string>(); // userId -> "give" или "take"

        // Поддержка пользователей
        private class SupportTicket
        {
            public long Id { get; init; }
            public long UserId { get; init; }
            public string Category { get; init; } = string.Empty;
            public bool IsOpen { get; set; } = true;
            public List<(bool FromUser, string Text, DateTime Time)> Messages { get; } = new();
        }

        private static long _nextTicketId = 1;
        private static readonly Dictionary<long, SupportTicket> _tickets = new(); // ticketId -> ticket
        private static readonly Dictionary<long, long> _userActiveTicket = new(); // userId -> ticketId
        private static readonly HashSet<long> _awaitingSupportCategory = new(); // users choosing category
        private static readonly Dictionary<long, long> _awaitingSupportMessageTicket = new(); // userId -> ticketId awaiting first message
        private static readonly Dictionary<long, (long ticketId, long userId)> _awaitingSupportReply = new(); // adminId -> (ticketId,userId)
        private static readonly long[] _supportAdminIds = { 1123842711 }; // ID админов поддержки

        private static string FormatUser(Telegram.Bot.Types.User user) => user.Username != null ? $"@{user.Username}" : $"ID:{user.Id}";

        private static readonly string[] _userAgentTemplates = {
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
		};

                private static string GenerateRandomUserAgent()
                {
                        var random = new Random();
                        var template = _userAgentTemplates[random.Next(_userAgentTemplates.Length)];
                        var chromeVersion = random.Next(118, 124);
                        var patchVersion = random.Next(0, 10);
                        return template.Replace("Chrome/120.0.0.0", $"Chrome/{chromeVersion}.0.{patchVersion}.0");
                }

        private static readonly string[] _whatsAppCodeSelectors =
        {
            "div[data-testid='ocf-code']",
            "div[data-testid='ocf-callout']",
            "div[data-testid='ocf-enter-code']",
            "div[data-testid='ocf-step']",
            "div[data-testid='code']",
            "div[data-testid*='code']",
            "section[data-testid*='code']",
            "span[data-testid*='code']",
            "div[aria-label*='код']",
            "span[aria-label*='код']",
            "div[aria-label*='code']",
            "span[aria-label*='code']",
            "div[class*='code']",
            "span[class*='code']",
            "div[data-animate-modal-body='true']",
            "div[data-animate-modal-body='true'] span",
            "div[role='dialog']",
            "div[role='dialog'] span",
            "code",
            "pre",
            "span[dir='ltr']",
            "span[dir='auto']"
        };

        private static readonly Regex HyphenCodeRegex = new(@"(?<!\w)([A-Z0-9]{4}-[A-Z0-9]{4,8})(?!\w)", RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);
        private static readonly Regex MultiDigitCodeRegex = new(@"(?<!\d)(?:\d[\s\u00A0\u202F\u205F\u2007\u2009\u200A\u2060\u2066-\u2069\u206A-\u206F\-]*){6,8}(?!\d)", RegexOptions.Compiled | RegexOptions.CultureInvariant);
        private static readonly Regex NonDigitRegex = new(@"[^\d]", RegexOptions.Compiled);
        private const string DirectionalMarksRaw = "\u200E\u200F\u202A\u202B\u202C\u202D\u202E\u2066\u2067\u2068\u2069\u206A\u206B\u206C\u206D\u206E\u206F\u200B\u200C\u200D\u2060\uFEFF";
        private static readonly HashSet<char> DirectionalMarks = new(DirectionalMarksRaw);
        private const string AdditionalWhitespaceRaw = "\u00A0\u202F\u205F\u2007\u2008\u2009\u200A\u3000";
        private static readonly char[] AdditionalWhitespaceCharacters = AdditionalWhitespaceRaw.ToCharArray();

        private static string? TryGetChromePath()
        {
            var candidates = new[]
            {
                Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), "Google", "Chrome", "Application", "chrome.exe"),
                Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFilesX86), "Google", "Chrome", "Application", "chrome.exe"),
                Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "Google", "Chrome", "Application", "chrome.exe")
            };
            foreach (var p in candidates)
            {
                if (System.IO.File.Exists(p)) return p;
            }
            return null;
        }

        private static void StartAuthorizationTimeout(long userId, long chatId, string userDataDir)
        {
            var cts = new CancellationTokenSource();
            if (!_authTimeoutCtsByUser.TryGetValue(userId, out var list))
            {
                list = new List<(string, CancellationTokenSource)>();
                _authTimeoutCtsByUser[userId] = list;
            }
            list.Add((userDataDir, cts));

            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(5), cts.Token);

                    if (_awaitingCodeSessionDirByUser.TryGetValue(userId, out var currentDir) && currentDir == userDataDir)
                    {
                        _awaitingCodeSessionDirByUser.Remove(userId);
                        _userPhoneNumbers.Remove(userId);
                    }

                    try
                    {
                        await using var cdp = await MaxWebAutomation.ConnectAsync(userDataDir, "web.max.ru");
                        await cdp.CloseBrowserAsync();
                    }
                    catch { }

                    CancelAuthorizationTimeout(userId, userDataDir);

                    try
                    {
                        await _botClient.SendTextMessageAsync(chatId, "⏱️ Авторизация отменена: время ожидания истекло.");
                    }
                    catch { }
                }
                catch (TaskCanceledException)
                {
                    // Таймер был отменён
                }
            });
        }

        private static void CancelAuthorizationTimeout(long userId, string? userDataDir = null)
        {
            if (_authTimeoutCtsByUser.TryGetValue(userId, out var list))
            {
                if (userDataDir == null)
                {
                    foreach (var (_, cts) in list)
                    {
                        try { cts.Cancel(); } catch { }
                    }
                    _authTimeoutCtsByUser.Remove(userId);
                }
                else
                {
                    var index = list.FindIndex(t => t.sessionDir == userDataDir);
                    if (index >= 0)
                    {
                        try { list[index].cts.Cancel(); } catch { }
                        list.RemoveAt(index);
                        if (list.Count == 0)
                            _authTimeoutCtsByUser.Remove(userId);
                    }
                }
            }
        }

        private static async Task<string> LaunchMaxWebAsync(string phone)
        {
            // Ждем доступного слота для браузера
            await _browserSemaphore.WaitAsync();
            
            try
            {
                var chrome = TryGetChromePath();
                var safePhone = new string((phone ?? "").Where(char.IsDigit).ToArray());
                // Создаем уникальный user-data-dir для каждого запуска
                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss_fff");
                var randomSuffix = Guid.NewGuid().ToString("N").Substring(0, 8);
                var userDir = Path.Combine(Path.GetTempPath(), $"max_web_{safePhone}_{timestamp}_{randomSuffix}");
                Directory.CreateDirectory(userDir);

                var userAgent = GenerateRandomUserAgent();
                Console.WriteLine($"[MAX] Запускаю Chrome для {phone} с User-Agent: {userAgent}");

                if (!string.IsNullOrEmpty(chrome))
                {
                    var args = $"--new-window --user-data-dir=\"{userDir}\" --remote-debugging-port=0 --user-agent=\"{userAgent}\" --disable-gpu --disable-software-rasterizer --disable-dev-shm-usage --disable-web-security --disable-features=VizDisplayCompositor --disable-background-timer-throttling --disable-backgrounding-occluded-windows --disable-renderer-backgrounding --disable-ipc-flooding-protection --memory-pressure-off --max_old_space_size=128 --disable-extensions --disable-plugins --disable-images --disable-animations --disable-video --disable-audio --disable-webgl --disable-canvas-aa --disable-2d-canvas-clip-aa --disable-accelerated-2d-canvas --disable-accelerated-jpeg-decoding --disable-accelerated-mjpeg-decode --disable-accelerated-video-decode --disable-accelerated-video-encode --disable-gpu-sandbox --disable-software-rasterizer --disable-background-networking --disable-default-apps --disable-sync --disable-translate --hide-scrollbars --mute-audio --no-first-run --no-default-browser-check --no-sandbox --disable-setuid-sandbox https://web.max.ru/";
                    var psi = new ProcessStartInfo
                    {
                        FileName = chrome,
                        Arguments = args,
                        UseShellExecute = false,
                        WorkingDirectory = Path.GetDirectoryName(chrome) ?? ""
                    };
                    Process.Start(psi);
                    Console.WriteLine($"[MAX] Открыл Chrome для {phone} с User-Agent: {userAgent} в папке: {Path.GetFileName(userDir)}");
                }
                else
                {
                    var psi = new ProcessStartInfo { FileName = "https://web.max.ru/", UseShellExecute = true };
                    Process.Start(psi);
                    Console.WriteLine($"[MAX] Chrome не найден, открыл URL в браузере по умолчанию для {phone}");
                }
                
                return userDir;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MAX] Ошибка запуска браузера: {ex.Message}");
                _browserSemaphore.Release();
                throw;
            }
        }
        


        private static async Task AutoFillPhoneAsync(string userDataDir, string phone, long telegramUserId, long chatId)
        {
            try
            {
                string digits = new string((phone ?? "").Where(char.IsDigit).ToArray());
                if (digits.StartsWith("+")) digits = digits.TrimStart('+');
                // Нормализуем под формат 9XXXXXXXXX
                if (digits.StartsWith("7")) digits = digits.Substring(1);
                if (digits.StartsWith("8")) digits = digits.Substring(1);
                if (digits.Length > 10) digits = digits.Substring(digits.Length - 10);
                if (digits.Length == 10 && digits[0] != '9')
                {
                    Console.WriteLine($"[MAX] Внимание: номер не начинается с 9: {digits}");
                }

                await Task.Delay(1500); // даем странице инициализироваться
                // Подключаемся к Chrome DevTools
                // Оптимизированные настройки для экономии ресурсов
                var optimizedSettings = new JObject
                {
                    ["args"] = new JArray
                    {
                        "--disable-gpu",
                        "--disable-software-rasterizer",
                        "--disable-dev-shm-usage",
                        "--disable-web-security",
                        "--disable-features=VizDisplayCompositor",
                        "--disable-background-timer-throttling",
                        "--disable-backgrounding-occluded-windows",
                        "--disable-renderer-backgrounding",
                        "--disable-ipc-flooding-protection",
                        "--memory-pressure-off",
                        "--max_old_space_size=128",
                        "--disable-extensions",
                        "--disable-plugins",
                        "--disable-images",
                        // "--disable-javascript", // Убираем, чтобы капча работала
                        // "--disable-css", // Убираем, чтобы капча отображалась
                        "--disable-animations",
                        "--disable-video",
                        "--disable-audio",
                        "--disable-webgl",
                        "--disable-canvas-aa",
                        "--disable-2d-canvas-clip-aa",
                        "--disable-accelerated-2d-canvas",
                        "--disable-accelerated-jpeg-decoding",
                        "--disable-accelerated-mjpeg-decode",
                        "--disable-accelerated-video-decode",
                        "--disable-accelerated-video-encode",
                        "--disable-gpu-sandbox",
                        "--disable-software-rasterizer",
                        "--disable-background-networking",
                        "--disable-default-apps",
                        "--disable-sync",
                        "--disable-translate",
                        "--hide-scrollbars",
                        "--mute-audio",
                        "--no-first-run",
                        "--no-default-browser-check",
                        "--no-sandbox",
                        "--disable-setuid-sandbox"
                    }
                };
                
                var cdp = await MaxWebAutomation.ConnectAsync(userDataDir, "web.max.ru", 15000, optimizedSettings);
                Console.WriteLine("[MAX] Подключился к CDP, проверяю статус подключения...");
                
                // Проверяем статус CDP подключения
                try
                {
                    var statusResult = await cdp.SendAsync("Runtime.evaluate", new JObject
                    {
                        ["expression"] = "console.log('CDP test'); 'CDP OK'",
                        ["returnByValue"] = true
                    });
                    Console.WriteLine($"[MAX] CDP статус: {statusResult}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[MAX] Ошибка CDP статуса: {ex.Message}");
                }
                
                // Включаем необходимые домены
                Console.WriteLine("[MAX] Включаю CDP домены...");
                try
                {
                    await cdp.EnableBasicDomainsAsync();
                    Console.WriteLine("[MAX] CDP домены включены");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[MAX] Ошибка включения доменов: {ex.Message}");
                }
                
                Console.WriteLine("[MAX] Подключился к CDP, жду 5 секунд для загрузки страницы...");
                await Task.Delay(5000);
                
                // Проверяем на капчу сразу после загрузки страницы
                Console.WriteLine("[MAX] Проверяю на капчу после загрузки...");
                bool captchaHandled = false;
                
                // Первая проверка
                captchaHandled = await CheckAndHandleCaptchaAsync(cdp, "после загрузки");
                
                // Если капча не найдена, ждем еще и проверяем снова (динамическая загрузка)
                if (!captchaHandled)
                {
                    Console.WriteLine("[MAX] Жду еще 3 секунды для динамической загрузки капчи...");
                    await Task.Delay(3000);
                    captchaHandled = await CheckAndHandleCaptchaAsync(cdp, "после дополнительного ожидания");
                }
                
                // Если капча была обработана, ждем 5 секунд перед вводом номера
                if (captchaHandled)
                {
                    Console.WriteLine("[MAX] Капча обработана, жду 5 секунд перед вводом номера...");
                    await Task.Delay(5000);
                }
                
                Console.WriteLine("[MAX] Начинаю ввод номера...");
                                const string inputSelector = "input.field";
                                await cdp.FocusSelectorAsync(inputSelector);
                                await cdp.ClearInputAsync(inputSelector);
                                await cdp.TypeTextAsync(digits);
                                Console.WriteLine($"[MAX] Ввел номер {digits}");

                                // Кликаем по кнопке "Войти" (по тексту), при необходимости используем резервный селектор
                                await Task.Delay(300);
                                bool clicked = await cdp.ClickButtonByTextAsync("Войти");
                                if (!clicked)
                                {
                                    const string submitSelector = "button.button.button--large.button--neutral-primary.button--stretched";
                                    clicked = await cdp.ClickSelectorAsync(submitSelector);
                                }
                                Console.WriteLine(clicked ? "[MAX] Нажал кнопку Войти" : "[MAX] Не удалось нажать кнопку Войти");

				                // Проверяем на капчу после ввода номера
                Console.WriteLine("[MAX] Проверяю на капчу после ввода номера...");
                try
                {
                    var captchaCheck2 = await cdp.SendAsync("Runtime.evaluate", new JObject
                    {
                        ["expression"] = @"
                            (function() {
                                try {
                                    // Ищем модальное окно с капчей
                                    var captchaModal = document.querySelector('.modal');
                                    if (captchaModal) {
                                        var continueButton = captchaModal.querySelector('button.start, button[class*=""start""], button:contains(""Продолжить""), button:contains(""Continue"")');
                                        if (continueButton) {
                                            console.log('Капча обнаружена после ввода номера, нажимаю кнопку Продолжить');
                                            continueButton.click();
                                            return { found: true, clicked: true, buttonText: continueButton.textContent };
                                        }
                                    }
                                    
                                    // Поиск по тексту кнопок
                                    var buttons = Array.from(document.querySelectorAll('button'));
                                    var continueBtn = buttons.find(btn => 
                                        btn.textContent.includes('Продолжить') || 
                                        btn.textContent.includes('Continue') ||
                                        btn.textContent.includes('Проверить') ||
                                        btn.textContent.includes('Verify')
                                    );
                                    
                                    if (continueBtn) {
                                        console.log('Кнопка продолжения найдена по тексту, нажимаю');
                                        continueBtn.click();
                                        return { found: true, clicked: true, buttonText: continueBtn.textContent };
                                    }
                                    
                                    return { found: false, clicked: false };
                                } catch(e) {
                                    return { error: e.message };
                                }
                            })()
                        ",
                        ["returnByValue"] = true
                    });
                    
                    if (captchaCheck2?["result"]?["result"]?["value"] != null)
                    {
                        var captchaResult2 = captchaCheck2["result"]["result"]["value"];
                        if (captchaResult2["found"]?.Value<bool>() == true && captchaResult2["clicked"]?.Value<bool>() == true)
                        {
                            Console.WriteLine($"[MAX] ✅ Капча после ввода номера обработана! Кнопка: {captchaResult2["buttonText"]?.Value<string>()}");
                            Console.WriteLine("[MAX] Капча после ввода номера обработана, жду 5 секунд...");
                            await Task.Delay(5000); // Ждем 5 секунд после обработки капчи
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[MAX] Ошибка проверки капчи после ввода номера: {ex.Message}");
                }
                
                // Проверяем на фрод-селектор (слишком много попыток)
                Console.WriteLine("[MAX] Проверяю на фрод-селектор...");
                await Task.Delay(5000); // Увеличиваем время ожидания до 5 секунд
				
				try
				{
					var fraudCheck = await cdp.SendAsync("Runtime.evaluate", new JObject
					{
						["expression"] = @"
							(function() {
								try {
									var bodyText = document.body ? document.body.textContent : '';
									return {
										bodyText: bodyText || 'EMPTY BODY'
									};
								} catch(e) {
									return { error: e.message };
								}
							})()
						",
						["returnByValue"] = true
					});
					
					if (fraudCheck?["result"]?["result"]?["value"] != null)
					{
						var fraudResult = fraudCheck["result"]["result"]["value"];
						if (fraudResult["error"] == null)
						{
							var bodyTextToken = fraudCheck?["result"]?["result"]?["value"]?["bodyText"];
							var rawBodyText = bodyTextToken?.ToString() ?? "";
							var bodyText = rawBodyText.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Trim();
							
							var hasFraudText = bodyText.Contains("Попробуйте позже") || 
											   bodyText.Contains("Слишком много попыток") ||
											   bodyText.Contains("Too many attempts") ||
											   bodyText.Contains("Try again later") ||
											   bodyText.Contains("Превышен лимит") ||
											   bodyText.Contains("Limit exceeded") ||
											   bodyText.Contains("Блокировка") ||
											   bodyText.Contains("Blocked");
							
							if (hasFraudText)
							{
								Console.WriteLine("[MAX] 🚨 ФРОД ОБНАРУЖЕН! Слишком много попыток");
								
								// Закрываем браузер
								try { await cdp.CloseBrowserAsync(); } catch {}
								
								// Отправляем сообщение в Telegram
								try 
								{ 
									await _botClient.SendTextMessageAsync(chatId, 
										"🚨 **ФРОД ОБНАРУЖЕН!**\n\n" +
										"На номере `" + phone + "` обнаружена блокировка.\n\n" +
										"⚠️ **Действие отменено**\n" +
										"🔒 Запустите прогрев позже или используйте другой номер.\n\n" +
										"💡 Рекомендации:\n" +
										"• Подождите 1-2 часа\n" +
										"• Используйте другой номер\n" +
										"• Проверьте статус номера\n\n" +
										"📝 Причина: Слишком много попыток входа");
								} 
								catch {}
								
								return; // Выходим из функции
							}
						}
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine($"[MAX] Ошибка проверки фрод-селектора: {ex.Message}");
				}
				
				Console.WriteLine("[MAX] Фрод не обнаружен, продолжаю...");

				Console.WriteLine("[MAX] Жду 3 секунды после клика для загрузки страницы...");
				await Task.Delay(3000);
				
				// Ждем изменения DOM после клика (MAX - это SPA)
				Console.WriteLine("[MAX] Жду изменения DOM после клика...");
				bool domChanged = false;
				var initialBodyText = "";
				
				// Сначала проверим, работает ли JavaScript вообще
 				Console.WriteLine("[MAX] Проверяю работу JavaScript...");
 				// Простой тест JavaScript
 				try
 				{
 					var simpleTest = await cdp.SendAsync("Runtime.evaluate", new JObject
 					{
 						["expression"] = "document.readyState",
 						["returnByValue"] = true
 					});
 					
 					if (simpleTest?["result"]?["value"] != null)
 					{
 						var readyState = simpleTest["result"]["value"].Value<string>();
 						Console.WriteLine($"[MAX] Document readyState: {readyState}");
 					}
 					else
 					{
 						Console.WriteLine("[MAX] Document readyState НЕ работает");
 					}
 				}
 				catch (Exception ex)
 				{
 					Console.WriteLine($"[MAX] Ошибка readyState: {ex.Message}");
 				}
 				
 				try
 				{
 					var jsTestResult = await cdp.SendAsync("Runtime.evaluate", new JObject
 					{
 						["expression"] = @"
 							(function() {
 								try {
 									var bodyText = document.body ? document.body.textContent : 'NO BODY';
 									var title = document.title || 'NO TITLE';
 									var url = window.location.href || 'NO URL';
 									var h3Elements = document.querySelectorAll('h3');
 									var pElements = document.querySelectorAll('p');
 							 return {
 									bodyText: bodyText || 'EMPTY BODY',
 									title: title,
 									url: url,
 									hasBody: !!document.body,
 									bodyLength: bodyText ? bodyText.length : 0,
 									h3Count: h3Elements.length,
 									pCount: pElements.length,
 									h3Texts: Array.from(h3Elements).map(el => el.textContent).slice(0, 3),
 									pTexts: Array.from(pElements).map(el => el.textContent).slice(0, 3)
 								};
 								} catch(e) {
 									return { error: e.message };
 								}
 							})()
 						",
 						["returnByValue"] = true
 					});
 					
 					if (jsTestResult?["result"]?["value"] != null)
 					{
 						var result = jsTestResult["result"]["value"];
 						if (result["error"] != null)
 						{
 							Console.WriteLine($"[MAX] JavaScript ошибка: {result["error"]}");
 						}
 						else
 						{
 							Console.WriteLine($"[MAX] JavaScript работает - получены данные");
 							Console.WriteLine($"[MAX] Body текст (первые 200 символов): {result["bodyText"]?.ToString().Substring(0, Math.Min(200, result["bodyText"]?.ToString().Length ?? 0))}...");
 						}
 					}
 					else
 					{
 						Console.WriteLine("[MAX] JavaScript вернул пустой результат");
 					}
 					
 					// Проверяем, есть ли уже экран кода на странице
 					Console.WriteLine("[MAX] Проверяю наличие экрана кода...");
 					try
 					{
 						// Прямое извлечение bodyText без Value<string>()
 						var bodyTextToken = jsTestResult?["result"]?["result"]?["value"]?["bodyText"];
 						var rawBodyText = bodyTextToken?.ToString() ?? "";
 						
 						// Простая очистка текста
 						var bodyText = rawBodyText.Replace("\n", " ").Replace("\r", " ").Replace("\t", " ").Trim();
							
 						var hasCodeText = bodyText.Contains("Код придёт");
 						var hasPhoneText = bodyText.Contains("Отправили код на");
 						var codeScreenFound = hasCodeText && hasPhoneText;
							
 						// Если экран кода найден, сразу запрашиваем код
						if (codeScreenFound)
						{
							Console.WriteLine("[MAX] 🎯 ЭКРАН КОДА ОБНАРУЖЕН! Запрашиваю код у пользователя");
                                                        _awaitingCodeSessionDirByUser[telegramUserId] = userDataDir;
                                                        _userPhoneNumbers[telegramUserId] = phone; // Сохраняем номер телефона
                                                        StartAuthorizationTimeout(telegramUserId, chatId, userDataDir);
                                                        var cancelKeyboard = new InlineKeyboardMarkup(new[]
                                                        {
                                                            new []
                                                            {
                                                                InlineKeyboardButton.WithCallbackData("❌ Отменить авторизацию", "cancel_auth")
                                                            }
                                                        });
                                                        try { await _botClient.SendTextMessageAsync(chatId, "✉️ Введите 6-значный код из MAX для входа.", replyMarkup: cancelKeyboard); } catch {}
                                                        return; // Выходим из функции, так как код уже найден
                                                }
                                        }
                                        catch (Exception ex)
                                        {
 						Console.WriteLine($"[MAX] Ошибка проверки экрана кода: {ex.Message}");
 					}
 				}
 				catch (Exception ex)
 				{
 					Console.WriteLine($"[MAX] Ошибка JavaScript анализа: {ex.Message}");
 				}
 				Console.WriteLine("[MAX] JavaScript анализ завершен");
 				
 				try
 				{
 					// Получаем начальный текст страницы
 					var initialResult = await cdp.SendAsync("Runtime.evaluate", new JObject
 					{
 						["expression"] = "document.body.textContent",
 						["returnByValue"] = true
 					});
 					initialBodyText = initialResult?["result"]?["value"]?.Value<string>() ?? "";
 					Console.WriteLine("[MAX] Получен начальный текст страницы");
 					
 					// Ждем изменения текста страницы (появления кода или ошибки)
 					for (int i = 0; i < 20; i++) // максимум 10 секунд
 					{
 						await Task.Delay(500);
 						var currentResult = await cdp.SendAsync("Runtime.evaluate", new JObject
 						{
 							["expression"] = "document.body.textContent",
 							["returnByValue"] = true
 						});
 						var currentBodyText = currentResult?["result"]?["value"]?.Value<string>() ?? "";
 						
 						if (currentBodyText != initialBodyText)
 						{
 							Console.WriteLine("[MAX] DOM изменился!");
 							domChanged = true;
 							break;
 						}
 					}
 					
 					if (!domChanged)
 					{
 						Console.WriteLine("[MAX] DOM не изменился за 10 секунд, продолжаю...");
 						
 						// Анализируем страницу ПОСЛЕ неудачного ожидания
 						Console.WriteLine("[MAX] Анализирую страницу после ожидания...");
 						try
 						{
 							var afterClickResult = await cdp.SendAsync("Runtime.evaluate", new JObject
 							{
 								["expression"] = @"
 									(function() {
 										try {
 											var bodyText = document.body ? document.body.textContent : 'NO BODY';
 											var title = document.title || 'NO TITLE';
 											var url = window.location.href || 'NO URL';
 											var h3Elements = document.querySelectorAll('h3');
 											var pElements = document.querySelectorAll('p');
 									 return {
 											bodyText: bodyText || 'EMPTY BODY',
 											title: title,
 											url: url,
 											hasBody: !!document.body,
 											bodyLength: bodyText ? bodyText.length : 0,
 											h3Count: h3Elements.length,
 											pCount: pElements.length,
 											h3Texts: Array.from(h3Elements).map(el => el.textContent).slice(0, 3),
 											pTexts: Array.from(pElements).map(el => el.textContent).slice(0, 3)
 										};
 										} catch(e) {
 											return { error: e.message };
 										}
 									})()
 								",
 								["returnByValue"] = true
 							});
 							
 							if (afterClickResult?["result"]?["value"] != null)
 							{
 								var result = afterClickResult["result"]["value"];
 								if (result["error"] != null)
 								{
 									Console.WriteLine($"[MAX] JavaScript ошибка: {result["error"]}");
 								}
 								else
 								{
 									Console.WriteLine("[MAX] JavaScript анализ после ожидания завершен");
 								}
 							}
 							else
 							{
 								Console.WriteLine("[MAX] JavaScript анализ после ожидания не дал результатов");
 							}
 						}
 						catch (Exception ex)
 						{
 							Console.WriteLine($"[MAX] Ошибка анализа после ожидания: {ex.Message}");
 						}
 						Console.WriteLine("[MAX] Анализ после ожидания завершен");
 					}
 				}
 				catch (Exception ex)
 				{
 					Console.WriteLine($"[MAX] Ошибка ожидания DOM: {ex.Message}");
 				}

				// CDP ресурсы освободятся автоматически
 
 				Console.WriteLine("[MAX] Начинаю ожидание экрана ввода кода...");
				// Надежное ожидание экрана кода с переподключением при ошибках
				bool seen = false;
				
				// Сначала попробуем найти элементы через JavaScript
				Console.WriteLine("[MAX] Пробую найти элементы через JavaScript...");
				try
				{
					var jsResult = await cdp.SendAsync("Runtime.evaluate", new JObject
					{
						["expression"] = @"
							(function() {
								var h3 = document.querySelector('h3.svelte-1wkbz16');
								var p = document.querySelector('p.svelte-1wkbz16');
								var hasCodeText = document.body.textContent.includes('Код придёт');
								var hasErrorText = document.body.textContent.includes('Если номер неверный');
								
								return {
									h3: !!h3,
									p: !!p,
									codeText: hasCodeText,
									errorText: hasErrorText,
									bodyText: document.body.textContent.substring(0, 200)
								};
							})()
						",
						["awaitPromise"] = true,
						["returnByValue"] = true
					});
					
					if (jsResult?["result"]?["value"] != null)
					{
						var result = jsResult["result"]["value"];
						Console.WriteLine($"[MAX] JavaScript результат: h3={result["h3"]}, p={result["p"]}, codeText={result["codeText"]}, errorText={result["errorText"]}");
						Console.WriteLine($"[MAX] Первые 200 символов body: {result["bodyText"]}");
						
						seen = result["h3"]?.Value<bool>() == true || 
							   result["p"]?.Value<bool>() == true || 
							   result["codeText"]?.Value<bool>() == true || 
							   result["errorText"]?.Value<bool>() == true;
					}
				}
				catch (Exception ex)
				{
					Console.WriteLine($"[MAX] Ошибка JavaScript поиска: {ex.Message}");
				}
				
				if (seen)
				{
					Console.WriteLine("[MAX] Элементы найдены через JavaScript!");
				}
				else
				{
					Console.WriteLine("[MAX] JavaScript не нашел элементы, пробую CDP методы...");
				}
				
				for (int attempt = 1; attempt <= 2 && !seen; attempt++)
				{
					Console.WriteLine($"[MAX] Попытка {attempt} ожидания экрана кода");
					try
					{
						Console.WriteLine("[MAX] Проверяю селектор h3.svelte-1wkbz16...");
						var seenH3 = await cdp.WaitForSelectorAsync("h3.svelte-1wkbz16", timeoutMs: 15000);
						Console.WriteLine($"[MAX] Результат h3: {seenH3}");
						var seenText = seenH3 ? true : await cdp.WaitForBodyTextContainsAsync("Код придёт", timeoutMs: 15000);
						Console.WriteLine($"[MAX] Результат текста: {seenText}");
						var seenP = (seenH3 || seenText) ? true : await cdp.WaitForSelectorAsync("p.svelte-1wkbz16", timeoutMs: 5000);
						Console.WriteLine($"[MAX] Результат p: {seenP}");
						var seenPText = (seenH3 || seenText || seenP) ? true : await cdp.WaitForBodyTextContainsAsync("Если номер неверный", timeoutMs: 5000);
						Console.WriteLine($"[MAX] Результат p текста: {seenPText}");
						seen = seenH3 || seenText || seenP || seenPText;
						Console.WriteLine($"[MAX] Итоговый результат попытки {attempt}: {seen}");
					}
					catch (Exception ex)
					{
						Console.WriteLine($"[MAX] Ошибка ожидания экрана кода (попытка {attempt}): {ex.Message}");
						Console.WriteLine($"[MAX] Stack trace: {ex.StackTrace}");
						await Task.Delay(500);
						// попробуем переподключиться и проверить ещё раз
						try
						{
							Console.WriteLine($"[MAX] Переподключение к CDP для попытки {attempt}...");
							await using var cdp2 = await MaxWebAutomation.ConnectAsync(userDataDir, "web.max.ru");
							Console.WriteLine($"[MAX] Переподключение успешно, проверяю экран кода...");
							var seenH32 = await cdp2.WaitForSelectorAsync("h3.svelte-1wkbz16", timeoutMs: 8000);
							Console.WriteLine($"[MAX] Результат h3 после переподключения: {seenH32}");
							var seenText2 = seenH32 ? true : await cdp2.WaitForBodyTextContainsAsync("Код придёт", timeoutMs: 8000);
							Console.WriteLine($"[MAX] Результат текста после переподключения: {seenText2}");
							var seenP2 = (seenH32 || seenText2) ? true : await cdp2.WaitForSelectorAsync("p.svelte-1wkbz16", timeoutMs: 4000);
							Console.WriteLine($"[MAX] Результат p после переподключения: {seenP2}");
							var seenPText2 = (seenH32 || seenText2 || seenP2) ? true : await cdp2.WaitForBodyTextContainsAsync("Если номер неверный", timeoutMs: 4000);
							Console.WriteLine($"[MAX] Результат p текста после переподключения: {seenPText2}");
							seen = seenH32 || seenText2 || seenP2 || seenPText2;
							Console.WriteLine($"[MAX] Итоговый результат после переподключения: {seen}");
						}
						catch (Exception ex2)
						{
							Console.WriteLine($"[MAX] Повторная ошибка ожидания экрана кода: {ex2.Message}");
							Console.WriteLine($"[MAX] Stack trace повторной ошибки: {ex2.StackTrace}");
						}
					}
				}

				Console.WriteLine($"[MAX] Завершил ожидание экрана кода. Результат: {seen}");
				if (seen)
				{
                                        Console.WriteLine("[MAX] Обнаружено сообщение о коде подтверждения");
                                        _awaitingCodeSessionDirByUser[telegramUserId] = userDataDir;
                                        _userPhoneNumbers[telegramUserId] = phone; // Сохраняем номер телефона
                                        StartAuthorizationTimeout(telegramUserId, chatId, userDataDir);
                                        var cancelKb = new InlineKeyboardMarkup(new[]
                                        {
                                            new []
                                            {
                                                InlineKeyboardButton.WithCallbackData("❌ Отменить авторизацию", "cancel_auth")
                                            }
                                        });
                                        try { await _botClient.SendTextMessageAsync(chatId, "✉️ Введите 6-значный код из MAX для входа.", replyMarkup: cancelKb); } catch {}
                                }
                                else
                                {
                                        Console.WriteLine("[MAX] Не дождался экрана ввода кода, отправляю запрос на код по таймауту");
                                        _awaitingCodeSessionDirByUser[telegramUserId] = userDataDir;
                                        _userPhoneNumbers[telegramUserId] = phone; // Сохраняем номер телефона
                                        StartAuthorizationTimeout(telegramUserId, chatId, userDataDir);
                                        var cancelKb2 = new InlineKeyboardMarkup(new[]
                                        {
                                            new []
                                            {
                                                InlineKeyboardButton.WithCallbackData("❌ Отменить авторизацию", "cancel_auth")
                                            }
                                        });
                                        try { await _botClient.SendTextMessageAsync(chatId, "✉️ Введите 6-значный код из MAX для входа.", replyMarkup: cancelKb2); } catch {}
                                }
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine($"[MAX] Ошибка автозаполнения номера: {ex.Message}");
                                // На случай падения CDP всё равно попросим код, если пользователь уже нажал Войти
                                try
                                {
                                        _awaitingCodeSessionDirByUser[telegramUserId] = userDataDir;
                                        _userPhoneNumbers[telegramUserId] = phone; // Сохраняем номер телефона
                                        StartAuthorizationTimeout(telegramUserId, chatId, userDataDir);
                                        var cancelKb3 = new InlineKeyboardMarkup(new[]
                                        {
                                            new []
                                            {
                                                InlineKeyboardButton.WithCallbackData("❌ Отменить авторизацию", "cancel_auth")
                                            }
                                        });
                                        await _botClient.SendTextMessageAsync(chatId, "✉️ Введите 6-значный код из MAX для входа.", replyMarkup: cancelKb3);
                                }
                                catch {}
                        }
                }

        private static async Task<bool> TryHandleLoginCodeAsync(Message message, CancellationToken cancellationToken)
        {
            if (message.From == null) return false;
            // Обрабатываем код ТОЛЬКО если явно ждём его
            var awaiting = _awaitingCodeSessionDirByUser.TryGetValue(message.From.Id, out var userDataDir);
            if (!awaiting) return false;
            var digitsOnly = new string((message.Text ?? string.Empty).Where(char.IsDigit).ToArray());
            if (digitsOnly.Length != 6)
            {
                await _botClient.SendTextMessageAsync(message.Chat.Id, "Введите ровно 6 цифр.", cancellationToken: cancellationToken);
                return true; // перехватываем, пока ждём код
            }
            // 6 цифр — у нас есть актуальная сессия в userDataDir из ожидания
            try
            {
                await using var cdp = await MaxWebAutomation.ConnectAsync(userDataDir, "web.max.ru");
                // Пытаемся заполнить конкретные input'ы OTP
                var filled = await cdp.FillOtpInputsAsync(digitsOnly);
                if (!filled)
                {
                    // Фолбэк: клик по контейнеру и печать текста
                    await cdp.WaitForSelectorAsync("div.code");
                    await cdp.ClickSelectorAsync("div.code");
                    await Task.Delay(100);
                    await cdp.TypeTextAsync(digitsOnly);
                    await Task.Delay(250);
                }
                // Пробуем нажать кнопку продолжения/входа
                var submitted = await cdp.SubmitFormBySelectorAsync("form.auth--code");
                if (!submitted)
                {
                    await cdp.ClickButtonByTextAsync("Продолжить");
                    await Task.Delay(200);
                    await cdp.ClickButtonByTextAsync("Войти");
                    await cdp.PressEnterAsync();
                }
                
                // Ждем загрузки страницы после отправки кода
                await Task.Delay(3000);
                
                // Проверяем на ошибку "Неверный код"
                try
                {
                    var errorCheck = await cdp.SendAsync("Runtime.evaluate", new JObject
                    {
                        ["expression"] = @"
							(function() {
								try {
									var errorElements = document.querySelectorAll('p.hint.hint--error');
									var errorTexts = Array.from(errorElements).map(el => el.textContent).join(' ');
									return {
										errorTexts: errorTexts || '',
										hasError: errorElements.length > 0
									};
								} catch(e) {
									return { error: e.message };
								}
							})()
						",
                        ["returnByValue"] = true
                    });
                    
                    if (errorCheck?["result"]?["result"]?["value"] != null)
                    {
                        var errorResult = errorCheck["result"]["result"]["value"];
                        if (errorResult["error"] == null)
                        {
                            var errorTexts = errorResult["hasError"]?.ToString() == "True";
                            var errorContent = errorResult["errorTexts"]?.ToString() ?? "";
                            
                            // Проверяем на неверный код
                            if (errorTexts && errorContent.Contains("Неверный код"))
                            {
                                Console.WriteLine("[MAX] 🚨 Обнаружена ошибка: Неверный код");
                                
                                // Очищаем поле ввода кода
                                try
                                {
                                    await cdp.WaitForSelectorAsync("div.code");
                                    await cdp.ClickSelectorAsync("div.code");
                                    await Task.Delay(100);
                                    await cdp.ClearInputAsync();
                                    await Task.Delay(100);
                                }
                                catch {}
                                
                                // Отправляем сообщение пользователю о неверном коде
                                var keyboard = new InlineKeyboardMarkup(new[]
                                {
                                    new []
                                    {
                                        InlineKeyboardButton.WithCallbackData("❌ Отменить авторизацию", "cancel_auth")
                                    }
                                });
                                
                                await _botClient.SendTextMessageAsync(message.Chat.Id, 
                                    "❌ **Код неверный!**\n\n" +
                                    "🔐 Введите новый 6-значный код из MAX.\n\n" +
                                    "💡 **Советы:**\n" +
                                    "• Проверьте правильность кода\n" +
                                    "• Код должен быть из последнего SMS\n" +
                                    "• Введите код без пробелов\n\n" +
                                    "📱 Отправьте новый код или отмените авторизацию:", 
                                    replyMarkup: keyboard,
                                    cancellationToken: cancellationToken);
                                
                                // НЕ удаляем сессию - пользователь может попробовать снова
                                return true;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[MAX] Ошибка проверки ошибок кода: {ex.Message}");
                }
                
                                // Если ошибок нет - код принят. Проверяем вход по селектору "h2.title.svelte-zqkpxo" и тексту "Чаты"
                await _botClient.SendTextMessageAsync(message.Chat.Id, "⏳ Проверяю вход...");
                // Даем сайту прогрузиться перед началом проверки
                try { await Task.Delay(10000, cancellationToken); } catch {}
 
                // Запускаем проверку входа в фоновом потоке, чтобы не блокировать Telegram бота
                _ = Task.Run(async () =>
                {
                    try
                    {
                        // Очищаем состояние ожидания кода сразу после начала проверки
                        CancelAuthorizationTimeout(message.From.Id, userDataDir);
                        _awaitingCodeSessionDirByUser.Remove(message.From.Id);

                        // Сохраняем сессию для повторной проверки входа
                        _verificationSessionDirByUser[message.From.Id] = userDataDir;
                        
                        // Создаем новое подключение к браузеру для проверки
                        await using var cdpForCheck = await MaxWebAutomation.ConnectAsync(userDataDir, "web.max.ru");
                        var chatsDetected = await CheckChatsScreenAsync(cdpForCheck, 90000, 300);

                if (chatsDetected)
                {
                    await _botClient.SendTextMessageAsync(message.Chat.Id, "✅ Вход выполнен! Обнаружен экран Чаты.", cancellationToken: cancellationToken);

                    // Получаем номер телефона пользователя
                    var phoneNumber = _userPhoneNumbers.TryGetValue(message.From.Id, out var phone) ? phone : string.Empty;

                    // Запускаем автоматизацию поиска по номеру
                            _ = Task.Run(async () => await AutomateFindByNumberAsync(userDataDir, phoneNumber, cancellationToken));

                    // Списываем 1 оплаченный запуск (если это не бесплатное возобновление)
                    var skipCharge = _resumeFreeByUser.TryGetValue(message.From.Id, out var resumedPhone) && !string.IsNullOrEmpty(resumedPhone) && _userPhoneNumbers.TryGetValue(message.From.Id, out var currentPhone) && currentPhone == resumedPhone;
                    if (!skipCharge)
                    {
                        try { await _supabaseService.TryConsumeOnePaidAccountAsync(message.From.Id); } catch { }
                    }
                    _resumeFreeByUser.Remove(message.From.Id);

                    // Стартуем 6-часовой прогрев для номера
                    var phoneForWarm = _userPhoneNumbers.TryGetValue(message.From.Id, out var pfw) ? pfw : null;
                    if (!string.IsNullOrEmpty(phoneForWarm))
                    {
                        StartWarmingTimer(phoneForWarm, message.Chat.Id);
                        try
                        {
                            var norm = SupabaseService.NormalizePhoneForActive(phoneForWarm);
                            if (!string.IsNullOrEmpty(norm))
                            {
                                var endsAt = _warmingEndsByPhone.TryGetValue(phoneForWarm, out var e) ? e : DateTime.UtcNow.AddHours(6);
                                await _supabaseService.InsertActiveNumberAsync(message.From.Id, norm, endsAt);
                            }
                        }
                        catch { }
                    }

                    // Очищаем номер телефона при подтвержденном входе
                    _userPhoneNumbers.Remove(message.From.Id);
                    
                    // Очищаем сессию проверки входа при успешном входе
                    _verificationSessionDirByUser.Remove(message.From.Id);
                }
                else
                {
                    var kb = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithCallbackData("🔄 Проверить снова", "verify_login") },
                        new [] { InlineKeyboardButton.WithCallbackData("❌ Отменить авторизацию", "cancel_auth") }
                    });
                    await _botClient.SendTextMessageAsync(message.Chat.Id,
                        "⚠️ Код принят, но пока не удалось подтвердить вход. Возможно, сайт ещё загружается или требуется дополнительное подтверждение.\n\nНажмите 'Проверить снова' через несколько секунд.",
                        replyMarkup: kb,
                        cancellationToken: cancellationToken);
                    // Сессию НЕ очищаем — дадим возможность проверить повторно
                }
                    }
                    catch (Exception ex)
                    {
                        await _botClient.SendTextMessageAsync(message.Chat.Id, $"❌ Ошибка проверки входа: {ex.Message}", cancellationToken: cancellationToken);
                        // Очищаем ожидание при ошибке
                        CancelAuthorizationTimeout(message.From.Id, userDataDir);
                        _awaitingCodeSessionDirByUser.Remove(message.From.Id);
                        _userPhoneNumbers.Remove(message.From.Id);

                        // Очищаем сессию проверки входа при ошибке
                        _verificationSessionDirByUser.Remove(message.From.Id);
                    }
                });
            }
            catch (Exception ex)
            {
                await _botClient.SendTextMessageAsync(message.Chat.Id, $"❌ Ошибка ввода кода: {ex.Message}", cancellationToken: cancellationToken);
                // Очищаем ожидание при ошибке
                CancelAuthorizationTimeout(message.From.Id, userDataDir);
                _awaitingCodeSessionDirByUser.Remove(message.From.Id);
                _userPhoneNumbers.Remove(message.From.Id); // Очищаем номер телефона
            }
            return true; // сообщение обработано
        }


        private static string RemoveDirectionalMarks(string text)
        {
            if (string.IsNullOrEmpty(text))
            {
                return text;
            }

            var builder = new StringBuilder(text.Length);
            foreach (var ch in text)
            {
                if (DirectionalMarks.Contains(ch))
                {
                    continue;
                }

                builder.Append(ch);
            }

            return builder.ToString();
        }

        private static string? ExtractWhatsAppCode(string? rawText)
        {
            if (string.IsNullOrWhiteSpace(rawText))
            {
                return null;
            }

            var sanitized = RemoveDirectionalMarks(rawText);
            foreach (var specialSpace in AdditionalWhitespaceCharacters)
            {
                sanitized = sanitized.Replace(specialSpace, ' ');
            }
            sanitized = sanitized.Trim();
            if (sanitized.Length == 0)
            {
                return null;
            }

            var hyphenMatch = HyphenCodeRegex.Match(sanitized);
            if (hyphenMatch.Success)
            {
                return hyphenMatch.Groups[1].Value.ToUpperInvariant();
            }

            var matches = MultiDigitCodeRegex.Matches(sanitized);
            foreach (Match match in matches)
            {
                var digits = NonDigitRegex.Replace(match.Value, string.Empty);
                // WhatsApp использует только 8-значные коды
                if (digits.Length == 8)
                {
                    Console.WriteLine($"[WA] ✅ Найден 8-значный код в тексте: {digits}");
                    return digits;
                }
            }

            var digitsOnly = NonDigitRegex.Replace(sanitized, string.Empty);
            // WhatsApp использует только 8-значные коды
            if (digitsOnly.Length == 8)
            {
                Console.WriteLine($"[WA] ✅ Найден 8-значный код (только цифры): {digitsOnly}");
                return digitsOnly;
            }

            return null;
        }

        private static async Task<string?> TryReadVerificationCodeAsync(MaxWebAutomation cdp)
        {
            try
            {
                // WhatsApp использует только 8-значные коды
                var scriptResult = await cdp.ExtractVerificationCodeAsync(8, 8);
                var fromScript = ExtractWhatsAppCode(scriptResult);
                if (!string.IsNullOrEmpty(fromScript))
                {
                    Console.WriteLine("[WA] Код найден через глубокий поиск в DOM.");
                    return fromScript;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WA] Ошибка скрипта извлечения кода: {ex.Message}");
            }

            try
            {
                var bodyText = await cdp.GetBodyTextAsync();
                var fromBody = ExtractWhatsAppCode(bodyText);
                if (!string.IsNullOrEmpty(fromBody))
                {
                    Console.WriteLine("[WA] Код найден в тексте страницы.");
                    return fromBody;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WA] Ошибка чтения текста страницы: {ex.Message}");
            }

            foreach (var selector in _whatsAppCodeSelectors)
            {
                try
                {
                    var text = await cdp.GetTextBySelectorAsync(selector);
                    var fromSelector = ExtractWhatsAppCode(text);
                    if (!string.IsNullOrEmpty(fromSelector))
                    {
                        Console.WriteLine($"[WA] Код найден по селектору {selector}.");
                        return fromSelector;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[WA] Ошибка чтения селектора {selector}: {ex.Message}");
                }
            }

            return null;
        }

        private static async Task<string> LaunchWhatsAppWebAsync(string phone, long telegramUserId, long chatId)
        {
            await _browserSemaphore.WaitAsync();

            try
            {
                var chrome = TryGetChromePath();
                var safePhone = new string((phone ?? "").Where(char.IsDigit).ToArray());
                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss_fff");
                var randomSuffix = Guid.NewGuid().ToString("N").Substring(0, 8);
                var userDir = Path.Combine(Path.GetTempPath(), $"wa_web_{safePhone}_{timestamp}_{randomSuffix}");
                Directory.CreateDirectory(userDir);

                var userAgent = GenerateRandomUserAgent();
                Console.WriteLine($"[WA] Запускаю Chrome для {phone} с User-Agent: {userAgent}");

                if (!string.IsNullOrEmpty(chrome))
                {
                    var args = $"--new-window --user-data-dir=\"{userDir}\" --remote-debugging-port=0 --user-agent=\"{userAgent}\" --disable-gpu --disable-software-rasterizer --disable-dev-shm-usage --disable-web-security --disable-features=VizDisplayCompositor --disable-background-timer-throttling --disable-backgrounding-occluded-windows --disable-renderer-backgrounding --disable-ipc-flooding-protection --memory-pressure-off --max_old_space_size=128 --disable-extensions --disable-plugins --disable-images --disable-animations --disable-video --disable-audio --disable-webgl --disable-canvas-aa --disable-2d-canvas-clip-aa --disable-accelerated-2d-canvas --disable-accelerated-jpeg-decoding --disable-accelerated-mjpeg-decode --disable-accelerated-video-decode --disable-accelerated-video-encode --disable-gpu-sandbox --disable-software-rasterizer --disable-background-networking --disable-default-apps --disable-sync --disable-translate --hide-scrollbars --mute-audio --no-first-run --no-default-browser-check --no-sandbox --disable-setuid-sandbox {WhatsAppWebUrl}";
                    var psi = new ProcessStartInfo
                    {
                        FileName = chrome,
                        Arguments = args,
                        UseShellExecute = false,
                        WorkingDirectory = Path.GetDirectoryName(chrome) ?? string.Empty
                    };
                    Process.Start(psi);
                    Console.WriteLine($"[WA] Открыл Chrome для {phone} с User-Agent: {userAgent} в папке: {Path.GetFileName(userDir)}");

                    await Task.Delay(10000); // ждём 10 секунд для прогрузки страницы

                    try
                    {
                        await using var cdp = await MaxWebAutomation.ConnectAsync(userDir, "web.whatsapp.com");
                        await cdp.ClickButtonByTextAsync("Войти по номеру телефона");
                        Console.WriteLine("[WA] Нажал на кнопку 'Войти по номеру телефона'");

                        // Ждём и вводим номер без проверки наличия поля
                        await Task.Delay(25000);
                        const string phoneInputSelector = "input[aria-label='Введите свой номер телефона.']";
                        var escapedSelector = phoneInputSelector.Replace("\\", "\\\\").Replace("'", "\\'");
                        // Имитируем действия пользователя: кликаем, выделяем текст, удаляем его и кликаем снова
                        await cdp.ClickSelectorAsync(phoneInputSelector);
                        await cdp.SendAsync("Runtime.evaluate", new JObject
                        {
                                ["expression"] =
                                    $"(function(){{var e=document.querySelector('{escapedSelector}');if(e){{e.select();document.execCommand('delete');e.dispatchEvent(new Event('input',{{bubbles:true}}));}}}})()"
                        });
                        await cdp.ClickSelectorAsync(phoneInputSelector);
                        // Формируем номер с символом '+' и вводим его посимвольно
                        var digitsOnlyPhone = new string(phone.Where(char.IsDigit).ToArray());
                        var formattedPhone = "+" + digitsOnlyPhone;
                        foreach (var ch in formattedPhone)
                        {
                                await cdp.TypeTextAsync(ch.ToString());
                                await Task.Delay(50);
                        }
                        Console.WriteLine($"[WA] Ввёл номер {formattedPhone}");

                        await Task.Delay(5000);
                        const string nextBtnSelector = "button.x889kno.x1a8lsjc.x13jy36j.x64bnmy.x1n2onr6.x1rg5ohu.xk50ysn.x1f6kntn.xyesn5m.x1rl75mt.x19t5iym.xz7t8uv.x13xmedi.x178xt8z.x1lun4ml.xso031l.xpilrb4.x13fuv20.x18b5jzi.x1q0q8m5.x1t7ytsu.x1v8p93f.x1o3jo1z.x16stqrj.xv5lvn5.x1hl8ikr.xfagghw.x9dyr19.x9lcvmn.xbtce8p.xcjl5na.x14v0smp.x1k3x3db.xgm1il4.xuxw1ft.xv52azi";
                        await cdp.ClickSelectorAsync(nextBtnSelector);
                        Console.WriteLine("[WA] Нажал кнопку 'Далее'");

                        string code = string.Empty;
                        try
                        {
                                var sw = Stopwatch.StartNew();
                                while (sw.ElapsedMilliseconds < 60000 && string.IsNullOrEmpty(code))
                                {
                                        var candidate = await TryReadVerificationCodeAsync(cdp);
                                        if (!string.IsNullOrEmpty(candidate))
                                        {
                                                code = candidate;
                                                break;
                                        }

                                        await Task.Delay(1000);
                                }

                                if (string.IsNullOrEmpty(code))
                                {
                                        Console.WriteLine("[WA] Код не найден на странице");
                                }
                        }
                        catch (Exception ex)
                        {
                                Console.WriteLine($"[WA] Ошибка получения кода: {ex.Message}");
                        }

                        _userPhoneNumbers[telegramUserId] = phone;
                        var cancelKb = new InlineKeyboardMarkup(new[]
                        {
                                new [] { InlineKeyboardButton.WithCallbackData("❌ Отменить авторизацию", "cancel_auth") }
                        });
                        if (!string.IsNullOrEmpty(code))
                        {
                                try
                                {
                                        await _botClient.SendTextMessageAsync(chatId, $"🔑 Код для номера {phone}:\n<code>{code}</code>", parseMode: ParseMode.Html, replyMarkup: cancelKb);
                                }
                                catch { }
                        }
                        else
                        {
                                Console.WriteLine("[WA] Код не получен, сообщение не отправлено");
                        }
                        }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[WA] Не удалось нажать на 'Войти по номеру телефона': {ex.Message}");
                    }
                }
                else
                {
                    var psi = new ProcessStartInfo { FileName = WhatsAppWebUrl, UseShellExecute = true };
                    Process.Start(psi);
                    Console.WriteLine($"[WA] Chrome не найден, открыл URL в браузере по умолчанию для {phone}");
                }

                return userDir;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WA] Ошибка запуска браузера: {ex.Message}");
                _browserSemaphore.Release();
                throw;
            }
        }

        static async Task Main(string[] args)
        {
            try
            {
                // Инициализация сервисов
                _supabaseService = new SupabaseService(_supabaseUrl, _supabaseKey);
                _cryptoPayService = new CryptoPayService(_cryptoPayToken);
                

                
                // Инициализация бота
                _botClient = new TelegramBotClient(_botToken);

                LoadWarmingState();

                // Создаем CTS и регистрируем обработчики завершения
                using var cts = new CancellationTokenSource();
                _cts = cts;

                AppDomain.CurrentDomain.ProcessExit += (_, _) =>
                {
                    try
                    {
                        Console.WriteLine("[WARMING] Сохраняю состояние перед завершением...");
                        SaveWarmingState();
                    }
                    catch { }
                };

                Console.CancelKeyPress += (_, e) =>
                {
                    try
                    {
                        Console.WriteLine("[WARMING] Отмена по Ctrl+C, сохраняю состояние...");
                        SaveWarmingState();
                    }
                    catch { }
                    try { _cts?.Cancel(); } catch { }
                    e.Cancel = true;
                };

                // Запускаем Telegram polling в фоновом таске
                var receiverOptions = new ReceiverOptions { AllowedUpdates = new[] { UpdateType.Message, UpdateType.CallbackQuery } };
                _botClient.StartReceiving(HandleUpdateAsync, HandlePollingErrorAsync, receiverOptions, cts.Token);

                var me = await _botClient.GetMeAsync();
                Console.WriteLine($"Бот {me.Username} запущен!");

                // Фоновая проверка оплат (пуллинг)
                _ = Task.Run(async () =>
                {
                    Console.WriteLine("[Polling] Старт фоновой проверки оплат");
                    while (!cts.IsCancellationRequested)
                    {
                        try
                        {
                            using var http = new HttpClient();
                            http.DefaultRequestHeaders.Add("apikey", _supabaseKey);
                            http.DefaultRequestHeaders.Add("Authorization", $"Bearer {_supabaseKey}");
                            var resp = await http.GetAsync($"{_supabaseUrl}/rest/v1/payments?status=eq.pending&select=*");
                            var json = await resp.Content.ReadAsStringAsync();
                            List<Payment> pending;
                            if (resp.IsSuccessStatusCode)
                            {
                                try
                                {
                                    var token = Newtonsoft.Json.Linq.JToken.Parse(json);
                                    pending = token.Type == Newtonsoft.Json.Linq.JTokenType.Array
                                        ? Newtonsoft.Json.JsonConvert.DeserializeObject<List<Payment>>(json) ?? new List<Payment>()
                                        : new List<Payment>();
                                }
                                catch
                                {
                                    Console.WriteLine($"[Polling] Ошибка парсинга payments: {json}");
                                    pending = new List<Payment>();
                                }
                            }
                            else
                            {
                                Console.WriteLine($"[Polling] Supabase payments error {resp.StatusCode}: {json}");
                                pending = new List<Payment>();
                            }
                            foreach (var p in pending)
                            {
                                var status = await _cryptoPayService.GetInvoiceStatusAsync(p.Hash);
                                if (status == "paid")
                                {
                                    Console.WriteLine($"[Polling] Invoice {p.Hash} оплачен. Зачисляю {p.Quantity}");
                                    await _supabaseService.AddPaidAccountsAsync(p.UserId, p.Quantity);
                                    await _supabaseService.MarkPaymentPaidAsync(p.Hash);
                                    try { await _botClient.SendTextMessageAsync(p.UserId, $"✅ Оплата получена. Зачислено {p.Quantity} аккаунтов."); } catch {}
                                }
                                else if (status == "expired" || (DateTime.UtcNow - p.CreatedAt.ToUniversalTime()) > TimeSpan.FromMinutes(10))
                                {
                                    Console.WriteLine($"[Polling] Invoice {p.Hash} просрочен/старше 10 минут. Помечаю как canceled и удаляю сообщение об оплате");
                                    await _supabaseService.MarkPaymentCanceledAsync(p.Hash);
                                    if (p.ChatId.HasValue && p.MessageId.HasValue)
                                    {
                                        try { await _botClient.DeleteMessageAsync(p.ChatId.Value, p.MessageId.Value); } catch {}
                                    }
                                }
                            }

                            var respTime = await http.GetAsync($"{_supabaseUrl}/rest/v1/time_payments?select=*");
                            var jsonTime = await respTime.Content.ReadAsStringAsync();
                            List<TimePayment> pendingTime;
                            if (respTime.IsSuccessStatusCode)
                            {
                                try
                                {
                                    var tokenTime = Newtonsoft.Json.Linq.JToken.Parse(jsonTime);
                                    pendingTime = tokenTime.Type == Newtonsoft.Json.Linq.JTokenType.Array
                                        ? Newtonsoft.Json.JsonConvert.DeserializeObject<List<TimePayment>>(jsonTime) ?? new List<TimePayment>()
                                        : new List<TimePayment>();
                                }
                                catch
                                {
                                    Console.WriteLine($"[Polling] Ошибка парсинга time_payments: {jsonTime}");
                                    pendingTime = new List<TimePayment>();
                                }
                            }
                            else
                            {
                                Console.WriteLine($"[Polling] Supabase time_payments error {respTime.StatusCode}: {jsonTime}");
                                pendingTime = new List<TimePayment>();
                            }
                            foreach (var tp in pendingTime)
                            {
                                var status = await _cryptoPayService.GetInvoiceStatusAsync(tp.Hash);
                                if (status == "paid")
                                {
                                    Console.WriteLine($"[Polling] Time invoice {tp.Hash} оплачен. Зачисляю {tp.Hours}ч на {tp.PhoneNumber}");
                                    AddWarmingHours(tp.PhoneNumber, tp.Hours, tp.UserId);
                                    await _supabaseService.DeleteTimePaymentByHashAsync(tp.Hash);
                                    try { await _botClient.SendTextMessageAsync(tp.UserId, $"✅ Оплата получена. Зачислено {tp.Hours}ч на {tp.PhoneNumber}."); } catch {}
                                }
                                else if (status == "expired" || (DateTime.UtcNow - tp.CreatedAt.ToUniversalTime()) > TimeSpan.FromMinutes(10))
                                {
                                    Console.WriteLine($"[Polling] Time invoice {tp.Hash} просрочен. Удаляю запись");
                                    await _supabaseService.DeleteTimePaymentByHashAsync(tp.Hash);
                                    if (tp.ChatId.HasValue && tp.MessageId.HasValue)
                                    {
                                        try { await _botClient.DeleteMessageAsync(tp.ChatId.Value, tp.MessageId.Value); } catch {}
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Polling] Ошибка: {ex.Message}");
                        }
                        await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
                    }
                }, cts.Token);

                Console.ReadLine();
                SaveWarmingState();
                cts.Cancel();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка при запуске бота: {ex.Message}");
                try { SaveWarmingState(); } catch { }
                Console.ReadLine();
                Environment.Exit(1);
            }
        }

        private static void RequestShutdown()
        {
            if (_isShuttingDown) return;
            _isShuttingDown = true;
            try { SaveWarmingState(); } catch { }
            try { _cts?.Cancel(); } catch {}
            Task.Run(async () => { await Task.Delay(500); Environment.Exit(0); });
        }

        private static async Task HandleUpdateAsync(ITelegramBotClient botClient, Update update, CancellationToken cancellationToken)
        {
            // Обработка callback'ов от кнопок
            if (update.CallbackQuery is { } callbackQuery)
            {
                await HandleCallbackQueryAsync(botClient, callbackQuery, cancellationToken);
                return;
            }

            // Обработка текстовых сообщений
            if (update.Message is not { } message)
                return;

            if (message.Text is not { } messageText)
                return;

            var chatId = message.Chat.Id;
            Console.WriteLine($"Получено сообщение: '{messageText}' от пользователя {message.From?.Id} ({message.From?.Username})");

            // Перехват ввода 6-значного кода авторизации
            if (await TryHandleLoginCodeAsync(message, cancellationToken))
                return;

            // Если админ включил режим рассылки — обрабатываем следующее сообщение
            if (message.From?.Id == 1123842711 && _awaitingBroadcastMode != BroadcastMode.None && !_isBroadcastInProgress)
            {
                _isBroadcastInProgress = true;
                var mode = _awaitingBroadcastMode;
                _awaitingBroadcastMode = BroadcastMode.None;
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await RunBroadcastAsync(botClient, message, mode, cancellationToken);
                    }
                    finally
                    {
                        _isBroadcastInProgress = false;
                    }
                });
                await botClient.SendTextMessageAsync(chatId, "🚀 Запускаю рассылку... Это может занять время.", cancellationToken: cancellationToken);
                return;
            }

            // Если включен режим обслуживания, блокируем всех кроме админа
            if (_maintenance && message.From?.Id != 1123842711)
            {
                await botClient.SendTextMessageAsync(chatId, "⏳ Бот временно на обслуживании. Попробуйте позже.", cancellationToken: cancellationToken);
                return;
            }

            // Обработка системы поддержки
            if (messageText == "/support")
            {
                if (_userActiveTicket.ContainsKey(message.From!.Id))
                {
                    var tid = _userActiveTicket[message.From.Id];
                    await botClient.SendTextMessageAsync(chatId, $"📝 У вас уже есть открытый тикет #{tid}. Напишите сообщение для продолжения или /close для закрытия.", cancellationToken: cancellationToken);
                }
                else
                {
                    _awaitingSupportCategory.Add(message.From.Id);
                    var kb = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithCallbackData("💳 Оплата", "support_cat_pay") },
                        new [] { InlineKeyboardButton.WithCallbackData("📦 Товары", "support_cat_goods") },
                        new [] { InlineKeyboardButton.WithCallbackData("❓ Другое", "support_cat_other") }
                    });
                    await botClient.SendTextMessageAsync(chatId, "Выберите категорию обращения:", replyMarkup: kb, cancellationToken: cancellationToken);
                }
                return;
            }
            if (messageText == "/close")
            {
                if (_userActiveTicket.TryGetValue(message.From!.Id, out var tId) && _tickets.TryGetValue(tId, out var t) && t.IsOpen)
                {
                    t.IsOpen = false;
                    _userActiveTicket.Remove(message.From.Id);
                    await botClient.SendTextMessageAsync(chatId, $"✅ Тикет #{t.Id} закрыт.", cancellationToken: cancellationToken);
                    foreach (var adminId in _supportAdminIds)
                    {
                        await botClient.SendTextMessageAsync(adminId, $"✅ Тикет #{t.Id} от {FormatUser(message.From!)} закрыт пользователем.", cancellationToken: cancellationToken);
                    }
                }
                else
                {
                    await botClient.SendTextMessageAsync(chatId, "У вас нет открытых тикетов.", cancellationToken: cancellationToken);
                }
                return;
            }
            if (messageText == "/cancel")
            {
                var removed = _awaitingSupportCategory.Remove(message.From!.Id);
                removed |= _awaitingSupportMessageTicket.Remove(message.From.Id);
                removed |= _awaitingSupportReply.Remove(message.From.Id);
                if (removed)
                {
                    await botClient.SendTextMessageAsync(chatId, "❌ Действие отменено.", cancellationToken: cancellationToken);
                    return;
                }
            }
            if (_awaitingSupportMessageTicket.TryGetValue(message.From!.Id, out var pendingTicketId))
            {
                _awaitingSupportMessageTicket.Remove(message.From.Id);
                var ticket = _tickets[pendingTicketId];
                ticket.Messages.Add((true, messageText, DateTime.UtcNow));
                await botClient.SendTextMessageAsync(chatId, $"✅ Ваше обращение зарегистрировано под номером #{ticket.Id}. Ожидайте ответа.", cancellationToken: cancellationToken);
                var adminKb = new InlineKeyboardMarkup(new[]
                {
                    new [] {
                        InlineKeyboardButton.WithCallbackData("✉️ Ответить", $"support_reply_{ticket.Id}_{ticket.UserId}"),
                        InlineKeyboardButton.WithCallbackData("✅ Закрыть", $"support_close_{ticket.Id}_{ticket.UserId}")
                    }
                });
                foreach (var adminId in _supportAdminIds)
                {
                    await botClient.SendTextMessageAsync(adminId, $"🆘 Новый тикет #{ticket.Id} от {FormatUser(message.From!)} в категории {ticket.Category}:\n{messageText}", replyMarkup: adminKb, cancellationToken: cancellationToken);
                }
                return;
            }
            if (_userActiveTicket.TryGetValue(message.From!.Id, out var activeTicketId))
            {
                var ticket = _tickets[activeTicketId];
                if (ticket.IsOpen)
                {
                    ticket.Messages.Add((true, messageText, DateTime.UtcNow));
                    var adminKb = new InlineKeyboardMarkup(new[]
                    {
                        new [] {
                            InlineKeyboardButton.WithCallbackData("✉️ Ответить", $"support_reply_{ticket.Id}_{ticket.UserId}"),
                            InlineKeyboardButton.WithCallbackData("✅ Закрыть", $"support_close_{ticket.Id}_{ticket.UserId}")
                        }
                    });
                    foreach (var adminId in _supportAdminIds)
                    {
                        await botClient.SendTextMessageAsync(adminId, $"📩 Сообщение по тикету #{ticket.Id} от {FormatUser(message.From!)}:\n{messageText}", replyMarkup: adminKb, cancellationToken: cancellationToken);
                    }
                    return;
                }
            }
            if (_awaitingSupportReply.TryGetValue(message.From!.Id, out var reply))
            {
                _awaitingSupportReply.Remove(message.From.Id);
                var (ticketId, userId) = reply;
                if (_tickets.TryGetValue(ticketId, out var ticket) && ticket.IsOpen)
                {
                    ticket.Messages.Add((false, messageText, DateTime.UtcNow));
                    await botClient.SendTextMessageAsync(userId, $"💬 Ответ поддержки по тикету #{ticketId}:\n{messageText}", cancellationToken: cancellationToken);
                    foreach (var adminId in _supportAdminIds)
                    {
                        if (adminId != message.From.Id)
                            await botClient.SendTextMessageAsync(adminId, $"📤 Ответ по тикету #{ticketId} от {FormatUser(message.From!)}:\n{messageText}", cancellationToken: cancellationToken);
                    }
                    await botClient.SendTextMessageAsync(chatId, "✅ Ответ отправлен пользователю.", cancellationToken: cancellationToken);
                }
                return;
            }
            if (messageText == "/tickets" && _supportAdminIds.Contains(message.From!.Id))
            {
                var open = _tickets.Values.Where(t => t.IsOpen).ToList();
                if (open.Count == 0)
                {
                    await botClient.SendTextMessageAsync(chatId, "Открытых тикетов нет.", cancellationToken: cancellationToken);
                }
                else
                {
                    var sb = new StringBuilder("📋 Открытые тикеты:\n");
                    foreach (var t in open)
                        sb.AppendLine($"#{t.Id} от {t.UserId} ({t.Category})");
                    await botClient.SendTextMessageAsync(chatId, sb.ToString(), cancellationToken: cancellationToken);
                }
                return;
            }

            if (messageText.StartsWith("/start"))
            {
                Console.WriteLine($"Получена команда /start от пользователя {message.From.Id} ({message.From.Username})");
                
                // Проверяем реферальный параметр
                string? referralCode = null;
                if (messageText.Contains(" "))
                {
                    var parts = messageText.Split(' ');
                    if (parts.Length > 1 && parts[1].StartsWith("ref"))
                    {
                        referralCode = parts[1].Substring(3); // Убираем "ref" префикс
                        Console.WriteLine($"[AFFILIATE] Обнаружен реферальный код: {referralCode}");
                    }
                }
                
                // Создаем или получаем пользователя в базе данных
                var user = await _supabaseService.GetOrCreateUserAsync(message.From.Id, message.From.Username ?? "Unknown");
                Console.WriteLine($"Пользователь в базе: ID={user.Id}, Username={user.Username}");
                
                // Обработка реферального кода
                if (!string.IsNullOrEmpty(referralCode))
                {
                    Console.WriteLine($"[AFFILIATE] Обнаружен реферальный код: {referralCode}");
                    
                    // Проверяем, что пользователь новый (не имеет реферера)
                    if (!user.ReferrerId.HasValue)
                    {
                        // Ищем реферера по коду
                        var referrer = await _supabaseService.GetUserByAffiliateCodeAsync(referralCode);
                        if (referrer != null && referrer.Id != user.Id)
                        {
                            // Привязываем пользователя к рефереру
                            var updateData = new { referrer_id = referrer.Id };
                            var json = JsonConvert.SerializeObject(updateData);
                            var content = new StringContent(json, Encoding.UTF8, "application/json");

                            var response = await _supabaseService.HttpClient.PatchAsync($"{_supabaseService.SupabaseUrl}/rest/v1/users?id=eq.{user.Id}", content);
                            if (response.IsSuccessStatusCode)
                            {
                                // Увеличиваем счетчик рефералов у реферера
                                var referrerUpdateData = new { referrals = referrer.Referrals + 1 };
                                var referrerJson = JsonConvert.SerializeObject(referrerUpdateData);
                                var referrerContent = new StringContent(referrerJson, Encoding.UTF8, "application/json");
                                await _supabaseService.HttpClient.PatchAsync($"{_supabaseService.SupabaseUrl}/rest/v1/users?id=eq.{referrer.Id}", referrerContent);

                                Console.WriteLine($"[AFFILIATE] ✅ Пользователь {user.Id} привязан к рефереру {referrer.Id}");
                            }
                        }
                        else
                        {
                            Console.WriteLine($"[AFFILIATE] ❌ Реферер не найден или пользователь пытается пригласить сам себя");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"[AFFILIATE] ❌ Пользователь {user.Id} уже имеет реферера, реферальная ссылка недействительна");
                    }
                }
                
                var welcomeMessage = $"Привет, {message.From.Username}! 👋\n\n" +
                                   "➡ Atlantis Grev — бот для прогрева аккаунтов MAX\n\n" +
                                   "Чтобы добавить аккаунт, нажми на кнопку ➕ Добавить аккаунт.\n\n" +
                                   "❓ Чтобы ознакомиться с работой бота, нажмите Информацию.";

                var keyboard = new InlineKeyboardMarkup(new[]
                {
                    new []
                    {
                        InlineKeyboardButton.WithCallbackData("➕ Добавить аккаунт", "add_account"),
                        InlineKeyboardButton.WithCallbackData("💳 Оплатить", "pay")
                    },
                    new []
                    {
                        InlineKeyboardButton.WithCallbackData("👤 Профиль", "profile"),
                        InlineKeyboardButton.WithCallbackData("📱 Мои аккаунты", "my_accounts")
                    },
                    new []
                    {
                        InlineKeyboardButton.WithCallbackData("👥 Партнерская программа", "affiliate")
                    },
                    new []
                    {
                        InlineKeyboardButton.WithCallbackData("ℹ️ Информация", "info"),
                        InlineKeyboardButton.WithCallbackData("🛠️ Техподдержка", "support")
                    }
                });

                try
                {
                    var sentMessage = await botClient.SendTextMessageAsync(
                        chatId: chatId,
                        text: welcomeMessage,
                        replyMarkup: keyboard,
                        cancellationToken: cancellationToken
                    );
                    Console.WriteLine($"Сообщение отправлено пользователю {chatId}, ID сообщения: {sentMessage.MessageId}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка отправки сообщения: {ex.Message}");
                }
            }
            else if (messageText == "/cancel_broadcast" && message.From?.Id == 1123842711)
            {
                _awaitingBroadcastMode = BroadcastMode.None;
                await botClient.SendTextMessageAsync(chatId, "❌ Режим рассылки отменён.", cancellationToken: cancellationToken);
            }
            else if (messageText == "/admin")
            {
                Console.WriteLine($"Получена команда /admin от пользователя {message.From?.Id}");
                // Проверяем, является ли пользователь администратором
                if (message.From?.Id == 1123842711) // Ваш ID
                {
                    Console.WriteLine("Пользователь является админом, показываю админ панель");
                    var adminMessage = "🔐 Админ панель\n\n" +
                                     "Выберите действие:";

                    var maintenanceLabel = _maintenance ? "🟢 Включить бота" : "⛔ Поставить на паузу";
                    var adminKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("👤 Выдать аккаунты", "give_accounts"),
                            InlineKeyboardButton.WithCallbackData("➖ Убавить аккаунты", "take_accounts"),
                            InlineKeyboardButton.WithCallbackData("📊 Статистика", "admin_stats")
                        },
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("⏱️ Выдать время", "give_time")
                        },
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("📢 Рассылка (копировать)", "admin_broadcast_copy"),
                            InlineKeyboardButton.WithCallbackData("🔁 Рассылка (переслать)", "admin_broadcast_forward")
                        },
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("👥 Управление рефералами", "manage_referrals"),
                            InlineKeyboardButton.WithCallbackData("⚙️ Настройки", "admin_settings")
                        },
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData(maintenanceLabel, "toggle_maintenance")
                        },
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu")
                        }
                    });

                    await botClient.SendTextMessageAsync(
                        chatId: chatId,
                        text: adminMessage,
                        replyMarkup: adminKeyboard,
                        cancellationToken: cancellationToken
                    );
                }
                else
                {
                    await botClient.SendTextMessageAsync(
                        chatId: chatId,
                        text: "❌ У вас нет доступа к админ панели",
                        cancellationToken: cancellationToken
                    );
                }
            }
            else if (messageText.StartsWith("/give ") && message.From?.Id == 1123842711)
            {
                // Команда выдачи аккаунтов: /give ID количество
                try
                {
                    var parts = messageText.Split(' ');
                    if (parts.Length == 3 && long.TryParse(parts[1], out var userId) && int.TryParse(parts[2], out var accounts))
                    {
                        var success = await _supabaseService.AddPaidAccountsAsync(userId, accounts);
                        if (success)
                        {
                            await botClient.SendTextMessageAsync(
                                chatId: chatId,
                                text: $"✅ Пользователю {userId} прибавлено {accounts} оплаченных аккаунтов",
                                cancellationToken: cancellationToken
                            );
                        }
                        else
                        {
                            await botClient.SendTextMessageAsync(
                                chatId: chatId,
                                text: $"❌ Ошибка при выдаче аккаунтов пользователю {userId}",
                                cancellationToken: cancellationToken
                            );
                        }
                    }
                    else
                    {
                        await botClient.SendTextMessageAsync(
                            chatId: chatId,
                            text: "❌ Неверный формат. Используйте: /give ID количество",
                            cancellationToken: cancellationToken
                        );
                    }
                }
                catch (Exception ex)
                {
                    await botClient.SendTextMessageAsync(
                        chatId: chatId,
                        text: $"❌ Ошибка: {ex.Message}",
                        cancellationToken: cancellationToken
                    );
                }
            }

            // В обработке количества создаем инвойс и сохраняем платеж
            else if ((message.From != null && _awaitingPaymentQtyUserIds.Contains(message.From.Id)) && int.TryParse(messageText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var qty) && qty >= 1 && qty <= 100)
            {
                Console.WriteLine($"Обработка количества для оплаты: {qty}");
                var amountUsdt = qty * PricePerAccountUsdt;
                var description = $"Оплата {qty} аккаунтов по {PricePerAccountUsdt:F2} USDT (итого {amountUsdt:F2} USDT)";

                var invoice = await _cryptoPayService.CreateInvoiceAsync(amountUsdt, "USDT", description);
                if (invoice != null && !string.IsNullOrEmpty(invoice.Url))
                {
                    var payKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithUrl("💰 Оплатить", invoice.Url) },
                        new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                    });

                    var paymentMsg = await botClient.SendTextMessageAsync(
                        chatId: chatId,
                        text: $"Счет создан на {amountUsdt:F2} USDT.\n\nОплатите по кнопке ниже. После оплаты баланс пополнится автоматически.",
                        replyMarkup: payKeyboard,
                        cancellationToken: cancellationToken
                    );

                    await _supabaseService.CreatePaymentAsync(message.From!.Id, invoice.Hash, qty, amountUsdt, chatId, paymentMsg.MessageId);
                }
                else
                {
                    await botClient.SendTextMessageAsync(chatId, "❌ Не удалось создать счет. Попробуйте позже.", cancellationToken: cancellationToken);
                }
                if (message.From != null) _awaitingPaymentQtyUserIds.Remove(message.From.Id);
            }
            else if (message.From?.Id == 1123842711 && _adminActionState.TryGetValue(message.From.Id, out var adminAction) && adminAction == "give_time_all")
            {
                if (int.TryParse(messageText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var hours) && hours >= 1 && hours <= 48)
                {
                    _adminActionState.Remove(message.From.Id);
                    var users = await _supabaseService.GetAllUsersAsync();
                    var numbers = 0;
                    foreach (var u in users.Where(u => u.PhoneNumbers != null && u.PhoneNumbers.Count > 0))
                    {
                        foreach (var phone in u.PhoneNumbers)
                        {
                            AddWarmingHours(phone, hours, u.Id);
                            numbers++;
                            try { await botClient.SendTextMessageAsync(u.Id, $"✅ Вам выдано {hours}ч на номер {phone}."); } catch {}
                        }
                    }
                    await botClient.SendTextMessageAsync(chatId, $"✅ Выдано {hours}ч на {numbers} номер(ов).", cancellationToken: cancellationToken);
                }
                else
                {
                    await botClient.SendTextMessageAsync(chatId, "❌ Введите число часов от 1 до 48.", cancellationToken: cancellationToken);
                }
            }
            else if (message.From?.Id == 1123842711 && _adminActionState.TryGetValue(message.From.Id, out adminAction) && adminAction == "give_time_user")
            {
                var parts = messageText.Split(' ');
                if (parts.Length == 2 && long.TryParse(parts[0], out var uid) && int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var hours) && hours >= 1 && hours <= 48)
                {
                    _adminActionState.Remove(message.From.Id);
                    var user = await _supabaseService.GetUserAsync(uid);
                    if (user != null && user.PhoneNumbers != null && user.PhoneNumbers.Count > 0)
                    {
                        foreach (var phone in user.PhoneNumbers)
                        {
                            AddWarmingHours(phone, hours, uid);
                            try { await botClient.SendTextMessageAsync(uid, $"✅ Вам выдано {hours}ч на номер {phone}."); } catch {}
                        }
                        await botClient.SendTextMessageAsync(chatId, $"✅ Выдал {hours}ч пользователю {uid} на {user.PhoneNumbers.Count} номер(ов).", cancellationToken: cancellationToken);
                    }
                    else
                    {
                        await botClient.SendTextMessageAsync(chatId, "❌ Пользователь не найден или не имеет номеров.", cancellationToken: cancellationToken);
                    }
                }
                else
                {
                    await botClient.SendTextMessageAsync(chatId, "❌ Неверный формат. Используйте: ID часы (1-48).", cancellationToken: cancellationToken);
                }
            }
            else if (message.From != null && _awaitingIntervalByUser.TryGetValue(message.From.Id, out var phoneForInterval))
            {
                if (TryParseInterval(messageText, out var minSec, out var maxSec))
                {
                    _awaitingIntervalByUser.Remove(message.From.Id);
                    SetMessageInterval(phoneForInterval, minSec, maxSec);
                    await botClient.SendTextMessageAsync(chatId, $"✅ Интервал для {phoneForInterval} установлен: {minSec}-{maxSec} сек.", cancellationToken: cancellationToken);
                }
                else
                {
                    await botClient.SendTextMessageAsync(chatId, "❌ Неверный формат. Введите интервал, например 30-120.", cancellationToken: cancellationToken);
                }
            }
            else if (message.From != null && _awaitingHoursByUser.TryGetValue(message.From.Id, out var phoneForHours)
                     && int.TryParse(messageText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var hours) && hours >= 1 && hours <= 48)
            {
                var amountUsdt = CalculateHoursPrice(hours);
                var description = $"Покупка {hours}ч для {phoneForHours}";
                var invoice = await _cryptoPayService.CreateInvoiceAsync(amountUsdt, "USDT", description);
                if (invoice != null && !string.IsNullOrEmpty(invoice.Url))
                {
                    var payKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithUrl("💰 Оплатить", invoice.Url) },
                        new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                    });

                    var paymentMsg = await botClient.SendTextMessageAsync(
                        chatId: chatId,
                        text: $"Счет создан на {amountUsdt:F2} USDT за {hours}ч для {phoneForHours}.\n\nОплатите по кнопке ниже.",
                        replyMarkup: payKeyboard,
                        cancellationToken: cancellationToken
                    );

                    await _supabaseService.CreateTimePaymentAsync(message.From.Id, phoneForHours, hours, invoice.Hash, chatId, paymentMsg.MessageId);
                }
                else
                {
                    await botClient.SendTextMessageAsync(chatId, "❌ Не удалось создать счет. Попробуйте позже.", cancellationToken: cancellationToken);
                }
                _awaitingHoursByUser.Remove(message.From.Id);
            }
            else if (message.From != null && _awaitingHoursByUser.ContainsKey(message.From.Id))
            {
                await botClient.SendTextMessageAsync(chatId, "❌ Введите число часов от 1 до 48.", cancellationToken: cancellationToken);
            }
            // Ввод номера телефона как раньше
            else if (message.From != null && (messageText.StartsWith("+") || (messageText.Length >= 10 && messageText.All(c => char.IsDigit(c) || c == '+' || c == '(' || c == ')' || c == '-' || c == ' '))) && !(message.From.Id == 1123842711 && messageText.Split(' ').Length == 2))
            {
                // Обработка ввода номера телефона после нажатия кнопки "Добавить аккаунт"
                Console.WriteLine($"Обрабатываю номер телефона: {messageText}");
                
                try
                {
                    var (success, resultMessage) = await _supabaseService.AddPhoneNumberAsync(message.From.Id, messageText);
                    
                    // Если номер уже существует, добавляем кнопки для навигации
                    if (!success && resultMessage.Contains("уже есть в ваших аккаунтах"))
                    {
                        var duplicateKeyboard = new InlineKeyboardMarkup(new[]
                        {
                            new [] { InlineKeyboardButton.WithCallbackData("📱 Мои аккаунты", "my_accounts") },
                            new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                        });
                        
                        await botClient.SendTextMessageAsync(
                            chatId: chatId,
                            text: resultMessage,
                            replyMarkup: duplicateKeyboard,
                            cancellationToken: cancellationToken
                        );
                    }
                    else
                    {
                        await botClient.SendTextMessageAsync(
                            chatId: chatId,
                            text: resultMessage,
                            cancellationToken: cancellationToken
                        );
                    }
                }
                catch (Exception ex)
                {
                    await botClient.SendTextMessageAsync(
                        chatId: chatId,
                        text: $"❌ Ошибка: {ex.Message}",
                        cancellationToken: cancellationToken
                    );
                }
            }
            // Обработка ввода для кнопок give/take: формат "ID количество"
            else if (message.From?.Id == 1123842711 && messageText.Split(' ').Length == 2)
            {
                var parts = messageText.Split(' ');
                if (long.TryParse(parts[0], out var uid) && int.TryParse(parts[1], out var delta))
                {
                    // Проверяем состояние админ-панели
                    if (_adminActionState.TryGetValue(message.From.Id, out var action))
                    {
                        bool success = false;
                        if (action == "give")
                        {
                            // Всегда прибавляем, независимо от знака
                            success = await _supabaseService.AddPaidAccountsAsync(uid, Math.Abs(delta));
                            await botClient.SendTextMessageAsync(chatId, success ? $"✅ Выдал {Math.Abs(delta)} аккаунтов пользователю {uid}" : "❌ Не удалось выдать", cancellationToken: cancellationToken);
                        }
                        else if (action == "take")
                        {
                            // Всегда убавляем, независимо от знака
                            success = await _supabaseService.DecreasePaidAccountsAsync(uid, Math.Abs(delta));
                            await botClient.SendTextMessageAsync(chatId, success ? $"✅ Убавил {Math.Abs(delta)} аккаунтов у {uid}" : "❌ Не удалось убавить", cancellationToken: cancellationToken);
                        }
                        
                        // Очищаем состояние после обработки
                        _adminActionState.Remove(message.From.Id);
                    }
                    else
                    {
                        // Если состояние не установлено, используем старую логику по знаку
                        if (delta >= 0)
                        {
                            var ok = await _supabaseService.AddPaidAccountsAsync(uid, delta);
                            await botClient.SendTextMessageAsync(chatId, ok ? $"✅ Выдал {delta} аккаунтов пользователю {uid}" : "❌ Не удалось выдать", cancellationToken: cancellationToken);
                        }
                        else
                        {
                            var ok = await _supabaseService.DecreasePaidAccountsAsync(uid, Math.Abs(delta));
                            await botClient.SendTextMessageAsync(chatId, ok ? $"✅ Убавил {Math.Abs(delta)} аккаунтов у {uid}" : "❌ Не удалось убавить", cancellationToken: cancellationToken);
                        }
                    }
                }
            }

        }

        private static async Task HandleCallbackQueryAsync(ITelegramBotClient botClient, CallbackQuery callbackQuery, CancellationToken cancellationToken)
        {
            var chatId = callbackQuery.Message.Chat.Id;
            var messageId = callbackQuery.Message.MessageId;

            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("support_cat_"))
            {
                if (_awaitingSupportCategory.Contains(callbackQuery.From.Id))
                {
                    _awaitingSupportCategory.Remove(callbackQuery.From.Id);
                    var key = callbackQuery.Data.Substring("support_cat_".Length);
                    var category = key switch
                    {
                        "pay" => "💳 Оплата",
                        "goods" => "📦 Товары",
                        _ => "❓ Другое"
                    };
                    var ticket = new SupportTicket { Id = _nextTicketId++, UserId = callbackQuery.From.Id, Category = category };
                    _tickets[ticket.Id] = ticket;
                    _userActiveTicket[callbackQuery.From.Id] = ticket.Id;
                    _awaitingSupportMessageTicket[callbackQuery.From.Id] = ticket.Id;
                    await botClient.EditMessageTextAsync(chatId, messageId, $"Категория: {category}\nОпишите вашу проблему и отправьте первым сообщением.", cancellationToken: cancellationToken);
                }
                return;
            }
            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("support_reply_"))
            {
                var parts = callbackQuery.Data.Substring("support_reply_".Length).Split('_');
                if (parts.Length == 2 && long.TryParse(parts[0], out var ticketId) && long.TryParse(parts[1], out var userId))
                {
                    _awaitingSupportReply[callbackQuery.From.Id] = (ticketId, userId);
                    await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Введите ответ", cancellationToken: cancellationToken);
                    await botClient.SendTextMessageAsync(chatId, $"✍️ Напишите ответ для тикета #{ticketId}.", cancellationToken: cancellationToken);
                }
                return;
            }
            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("support_close_"))
            {
                var parts = callbackQuery.Data.Substring("support_close_".Length).Split('_');
                if (parts.Length == 2 && long.TryParse(parts[0], out var ticketId) && long.TryParse(parts[1], out var userId))
                {
                    if (_tickets.TryGetValue(ticketId, out var ticket))
                    {
                        ticket.IsOpen = false;
                        _userActiveTicket.Remove(userId);
                        await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Тикет закрыт", cancellationToken: cancellationToken);
                        await botClient.SendTextMessageAsync(userId, $"✅ Ваш тикет #{ticketId} закрыт поддержкой.", cancellationToken: cancellationToken);
                        foreach (var adminId in _supportAdminIds)
                        {
                            if (adminId != callbackQuery.From.Id)
                                await botClient.SendTextMessageAsync(adminId, $"⚠️ Тикет #{ticketId} закрыт администратором {FormatUser(callbackQuery.From)}.", cancellationToken: cancellationToken);
                        }
                        await botClient.EditMessageTextAsync(chatId, messageId, $"Тикет #{ticketId} закрыт.", cancellationToken: cancellationToken);
                    }
                }
                return;
            }

            // Прямой хендлер для start_account:<phone>
            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("start_account:"))
            {
                var phone = callbackQuery.Data.Substring("start_account:".Length);
                Console.WriteLine($"Запуск аккаунта для номера {phone}");

                // Не допускаем повторный запуск, если авторизация уже ожидает код
                if (_awaitingCodeSessionDirByUser.ContainsKey(callbackQuery.From.Id))
                {
                    var activePhone = _userPhoneNumbers.TryGetValue(callbackQuery.From.Id, out var p) ? p : "неизвестный номер";
                    var kb = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithCallbackData("❌ Отменить авторизацию", "cancel_auth") }
                    });
                    try { await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Авторизация уже запущена", showAlert: true, cancellationToken: cancellationToken); } catch {}
                    await botClient.SendTextMessageAsync(chatId, $"⚠️ Авторизация номера {activePhone} уже выполняется. Введите код из MAX или отмените авторизацию.", replyMarkup: kb, cancellationToken: cancellationToken);
                    return;
                }

                if (_sessionDirByPhone.ContainsKey(phone))
                {
                    try { await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Авторизация для этого номера уже выполняется", showAlert: true, cancellationToken: cancellationToken); } catch {}
                    return;
                }

                // Проверяем: есть ли остаток времени на этом номере (бесплатное возобновление)
                var hasRemaining = _warmingRemainingByPhone.TryGetValue(phone, out var remain) && remain > TimeSpan.Zero;
                if (!hasRemaining)
                {
                    // Проверяем наличие оплаченных аккаунтов
                    try
                    {
                        var paid = await _supabaseService.GetPaidAccountsAsync(callbackQuery.From.Id);
                        if (paid <= 0)
                        {
                            await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Нет оплаченных запусков", showAlert: true, cancellationToken: cancellationToken);
                            var warnKb = new InlineKeyboardMarkup(new[]
                            {
                                new [] { InlineKeyboardButton.WithCallbackData("💳 Оплатить", "pay"), InlineKeyboardButton.WithCallbackData("← Назад", "my_accounts") }
                            });
                            await botClient.EditMessageTextAsync(chatId, messageId, "❌ У вас нет оплаченных запусков. Пополните баланс, чтобы запустить прогрев.", replyMarkup: warnKb, cancellationToken: cancellationToken);
                            return;
                        }
                    }
                    catch { }
                }
                else
                {
                    // Запоминаем, что это бесплатное возобновление, чтобы не списывать при удачной авторизации
                    _resumeFreeByUser[callbackQuery.From.Id] = phone;
                }

                try { await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, $"🚀 Запуск {phone}...", cancellationToken: cancellationToken); } catch { }
                _ = Task.Run(async () =>
                {
                    try
                    {
                        var type = GetWarmingType(phone);
                        string userDataDirBg;
                        if (type.Equals("WhatsApp", StringComparison.OrdinalIgnoreCase))
                        {
                            userDataDirBg = await LaunchWhatsAppWebAsync(phone, callbackQuery.From.Id, chatId);
                            _lastSessionDirByUser[callbackQuery.From.Id] = userDataDirBg;
                            _sessionDirByPhone[phone] = userDataDirBg;
                        }
                        else
                        {
                            userDataDirBg = await LaunchMaxWebAsync(phone);
                            _lastSessionDirByUser[callbackQuery.From.Id] = userDataDirBg;
                            _sessionDirByPhone[phone] = userDataDirBg; // Сохраняем директорию по номеру телефона
                            await AutoFillPhoneAsync(userDataDirBg, phone, callbackQuery.From.Id, chatId);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[MAX] Ошибка фонового запуска: {ex.Message}");
                    }
                });
                return;
            }
            // Открыть карточку номера: acc:<phone>
            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("acc:"))
            {
                var phone = callbackQuery.Data.Substring("acc:".Length);
                _awaitingIntervalByUser.Remove(callbackQuery.From.Id);
                var statusText = FormatWarmingText(phone);
                var type = GetWarmingType(phone);
                var cardText = $"📞 Номер: {phone}\nТип: {type}\n{statusText}";
                InlineKeyboardMarkup cardKb;
                if (_warmingCtsByPhone.ContainsKey(phone))
                {
                    cardKb = new InlineKeyboardMarkup(new[]
                    {
                        new [] {
                            InlineKeyboardButton.WithCallbackData("🛑 Остановить", $"stop_warming:{phone}"),
                            InlineKeyboardButton.WithCallbackData("🗑️ Удалить", $"delete_account:{phone}")
                        },
                        new [] { InlineKeyboardButton.WithCallbackData($"🌐 Тип: {type}", $"select_warming_type:{phone}") },
                        new [] { InlineKeyboardButton.WithCallbackData("⚙️ Настройки", $"warming_settings:{phone}") },
                        new [] { InlineKeyboardButton.WithCallbackData("🛒 Купить часы", $"buy_hours:{phone}") },
                        new [] { InlineKeyboardButton.WithCallbackData("← Назад", "my_accounts") }
                    });
                }
                else
                {
                    cardKb = new InlineKeyboardMarkup(new[]
                    {
                        new [] {
                            InlineKeyboardButton.WithCallbackData("▶️ Запустить", $"start_account:{phone}"),
                            InlineKeyboardButton.WithCallbackData("🗑️ Удалить", $"delete_account:{phone}")
                        },
                        new [] { InlineKeyboardButton.WithCallbackData($"🌐 Тип: {type}", $"select_warming_type:{phone}") },
                        new [] { InlineKeyboardButton.WithCallbackData("⚙️ Настройки", $"warming_settings:{phone}") },
                        new [] { InlineKeyboardButton.WithCallbackData("🛒 Купить часы", $"buy_hours:{phone}") },
                        new [] { InlineKeyboardButton.WithCallbackData("← Назад", "my_accounts") }
                    });
                }
                await botClient.EditMessageTextAsync(chatId, messageId, cardText, replyMarkup: cardKb, cancellationToken: cancellationToken);
                return;
            }

            // Выбор типа прогрева: select_warming_type:<phone>
            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("select_warming_type:"))
            {
                var phone = callbackQuery.Data.Substring("select_warming_type:".Length);
                var current = GetWarmingType(phone);
                var kb = new InlineKeyboardMarkup(new[]
                {
                    new [] {
                        InlineKeyboardButton.WithCallbackData("Max", $"set_warming_type:{phone}:Max"),
                        InlineKeyboardButton.WithCallbackData("WhatsApp", $"set_warming_type:{phone}:WhatsApp")
                    },
                    new [] { InlineKeyboardButton.WithCallbackData("← Назад", $"acc:{phone}") }
                });
                await botClient.EditMessageTextAsync(chatId, messageId,
                    $"Выберите тип прогрева для {phone} (текущий: {current})",
                    replyMarkup: kb, cancellationToken: cancellationToken);
                return;
            }

            // Установка типа прогрева: set_warming_type:<phone>:<type>
            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("set_warming_type:"))
            {
                var parts = callbackQuery.Data.Split(':');
                if (parts.Length >= 3)
                {
                    var phone = parts[1];
                    var type = parts[2];
                    _warmingTypeByPhone[phone] = type;
                    await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, $"Тип прогрева: {type}", cancellationToken: cancellationToken);

                    var statusText = FormatWarmingText(phone);
                    var cardType = GetWarmingType(phone);
                    var cardText = $"📞 Номер: {phone}\nТип: {cardType}\n{statusText}";
                    InlineKeyboardMarkup cardKb;
                    if (_warmingCtsByPhone.ContainsKey(phone))
                    {
                        cardKb = new InlineKeyboardMarkup(new[]
                        {
                            new [] {
                                InlineKeyboardButton.WithCallbackData("🛑 Остановить", $"stop_warming:{phone}"),
                                InlineKeyboardButton.WithCallbackData("🗑️ Удалить", $"delete_account:{phone}")
                            },
                            new [] { InlineKeyboardButton.WithCallbackData($"🌐 Тип: {cardType}", $"select_warming_type:{phone}") },
                            new [] { InlineKeyboardButton.WithCallbackData("⚙️ Настройки", $"warming_settings:{phone}") },
                            new [] { InlineKeyboardButton.WithCallbackData("🛒 Купить часы", $"buy_hours:{phone}") },
                            new [] { InlineKeyboardButton.WithCallbackData("← Назад", "my_accounts") }
                        });
                    }
                    else
                    {
                        cardKb = new InlineKeyboardMarkup(new[]
                        {
                            new [] {
                                InlineKeyboardButton.WithCallbackData("▶️ Запустить", $"start_account:{phone}"),
                                InlineKeyboardButton.WithCallbackData("🗑️ Удалить", $"delete_account:{phone}")
                            },
                            new [] { InlineKeyboardButton.WithCallbackData($"🌐 Тип: {cardType}", $"select_warming_type:{phone}") },
                            new [] { InlineKeyboardButton.WithCallbackData("⚙️ Настройки", $"warming_settings:{phone}") },
                            new [] { InlineKeyboardButton.WithCallbackData("🛒 Купить часы", $"buy_hours:{phone}") },
                            new [] { InlineKeyboardButton.WithCallbackData("← Назад", "my_accounts") }
                        });
                    }
                    await botClient.EditMessageTextAsync(chatId, messageId, cardText, replyMarkup: cardKb, cancellationToken: cancellationToken);
                }
                return;
            }

            // Настройки прогрева: warming_settings:<phone>
            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("warming_settings:"))
            {
                var phone = callbackQuery.Data.Substring("warming_settings:".Length);
                var (minSec, maxSec) = GetMessageInterval(phone);
                _awaitingIntervalByUser[callbackQuery.From.Id] = phone;
                var kb = new InlineKeyboardMarkup(new[]
                {
                    new [] { InlineKeyboardButton.WithCallbackData("❌ Отмена", $"acc:{phone}") }
                });
                await botClient.EditMessageTextAsync(chatId, messageId,
                    $"⚙️ Интервал сообщений для {phone}\nТекущий: {minSec}-{maxSec} сек\nВведите новый интервал в формате мин-макс:",
                    replyMarkup: kb, cancellationToken: cancellationToken);
                return;
            }

            // Покупка часов: buy_hours:<phone>
            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("buy_hours:"))
            {
                var phone = callbackQuery.Data.Substring("buy_hours:".Length);
                _awaitingHoursByUser[callbackQuery.From.Id] = phone;
                var kb = new InlineKeyboardMarkup(new[]
                {
                    new [] { InlineKeyboardButton.WithCallbackData("❌ Отмена", $"acc:{phone}") }
                });
                await botClient.EditMessageTextAsync(chatId, messageId, $"⏱️ Введите количество часов для {phone} (1-48):", replyMarkup: kb, cancellationToken: cancellationToken);
                return;
            }

            // Подтверждение удаления аккаунта: confirm_delete:<phone>
            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("confirm_delete:"))
            {
                var phone = callbackQuery.Data.Substring("confirm_delete:".Length);
                await HandleDeleteAccountAsync(botClient, callbackQuery, phone, cancellationToken);
                return;
            }

            // Удаление аккаунта: delete_account:<phone>
            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("delete_account:"))
            {
                var phone = callbackQuery.Data.Substring("delete_account:".Length);

                // Проверяем наличие оставшегося времени прогрева
                var hasWarming =
                    (_warmingEndsByPhone.TryGetValue(phone, out var ends) && ends > DateTime.UtcNow) ||
                    (_warmingRemainingByPhone.TryGetValue(phone, out var remain) && remain > TimeSpan.Zero);

                if (hasWarming)
                {
                    var warnText =
                        $"⚠️ На номере {phone} есть оставшееся время прогрева.\n" +
                        $"При удалении это время будет потеряно.\n\n" +
                        $"Удалить номер?";
                    var warnKb = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithCallbackData("✅ Удалить", $"confirm_delete:{phone}") },
                        new [] { InlineKeyboardButton.WithCallbackData("❌ Отмена", $"acc:{phone}") }
                    });

                    await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, cancellationToken: cancellationToken);
                    await botClient.EditMessageTextAsync(chatId, messageId, warnText, replyMarkup: warnKb, cancellationToken: cancellationToken);
                }
                else
                {
                    await HandleDeleteAccountAsync(botClient, callbackQuery, phone, cancellationToken);
                }
                return;
            }

            // Остановить прогрев: stop_warming:<phone>
            if (callbackQuery.Data != null && callbackQuery.Data.StartsWith("stop_warming:"))
            {
                var phone = callbackQuery.Data.Substring("stop_warming:".Length);
                // Останавливаем таймер и сохраняем остаток
                if (_warmingCtsByPhone.TryGetValue(phone, out var cts))
                {
                    try { cts.Cancel(); } catch { }
                    _warmingCtsByPhone.Remove(phone);
                }
                if (_warmingEndsByPhone.TryGetValue(phone, out var ends))
                {
                    var left = ends - DateTime.UtcNow;
                    if (left < TimeSpan.Zero) left = TimeSpan.Zero;
                    _warmingRemainingByPhone[phone] = left;
                    _warmingEndsByPhone.Remove(phone);
                    SaveWarmingState();
                }

                // Закрываем браузер по этому номеру, затем чистим профиль
                bool closed = false;
                try
                {
                    string? dir = null;
                    if (_sessionDirByPhone.TryGetValue(phone, out var byPhone) && !string.IsNullOrEmpty(byPhone))
                        dir = byPhone;
                    else if (_lastSessionDirByUser.TryGetValue(callbackQuery.From.Id, out var byUser) && !string.IsNullOrEmpty(byUser))
                        dir = byUser;

                    if (!string.IsNullOrEmpty(dir))
                    {
                        try
                        {
                            await using var cdp = await MaxWebAutomation.ConnectAsync(dir, "web.max.ru");
                            await cdp.CloseBrowserAsync();
                            closed = true;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[STOP] Ошибка закрытия через CDP: {ex.Message}");
                        }
                        // Пытаемся удалить папку профиля после закрытия
                        try
                        {
                            if (Directory.Exists(dir))
                            {
                                Directory.Delete(dir, true);
                                Console.WriteLine($"[STOP] Папка профиля удалена: {dir}");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[STOP] Не удалось удалить папку профиля: {ex.Message}");
                        }
                    }
                }
                catch { }
                finally { _sessionDirByPhone.Remove(phone); }

                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, closed ? "Прогрев остановлен" : "Не удалось закрыть браузер", cancellationToken: cancellationToken);

                try
                {
                    var norm = SupabaseService.NormalizePhoneForActive(phone);
                    if (!string.IsNullOrEmpty(norm))
                        await _supabaseService.DeleteActiveNumberByPhoneAsync(norm);
                }
                catch { }

                var statusText2 = FormatWarmingText(phone);
                var type2 = GetWarmingType(phone);
                var cardText = $"📞 Номер: {phone}\nТип: {type2}\n{statusText2}";
                InlineKeyboardMarkup cardKb = new InlineKeyboardMarkup(new[]
                {
                    new [] {
                        InlineKeyboardButton.WithCallbackData("▶️ Запустить", $"start_account:{phone}"),
                        InlineKeyboardButton.WithCallbackData("🗑️ Удалить", $"delete_account:{phone}")
                    },
                    new [] { InlineKeyboardButton.WithCallbackData($"🌐 Тип: {type2}", $"select_warming_type:{phone}") },
                    new [] { InlineKeyboardButton.WithCallbackData("⚙️ Настройки", $"warming_settings:{phone}") },
                    new [] { InlineKeyboardButton.WithCallbackData("🛒 Купить часы", $"buy_hours:{phone}") },
                    new [] { InlineKeyboardButton.WithCallbackData("← Назад", "my_accounts") }
                });
                await botClient.EditMessageTextAsync(chatId, messageId, cardText, replyMarkup: cardKb, cancellationToken: cancellationToken);
                return;
            }

            // Повторная проверка входа
            if (callbackQuery.Data == "verify_login")
            {
                // Проверяем сессию для повторной проверки входа
                if (_verificationSessionDirByUser.TryGetValue(callbackQuery.From.Id, out var userDataDir))
                {
                    // Запускаем проверку входа в фоновом потоке, чтобы не блокировать Telegram бота
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await using var cdp = await MaxWebAutomation.ConnectAsync(userDataDir, "web.max.ru");
                            var chats = await CheckChatsScreenAsync(cdp, 90000, 300);

                            if (chats)
                            {
                                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "✅ Вход подтвержден", cancellationToken: cancellationToken);
                                await botClient.EditMessageTextAsync(chatId, messageId, "✅ Вход выполнен! Обнаружен экран Чаты.", cancellationToken: cancellationToken);

                                // Получаем номер телефона пользователя
                                var phoneNumber = _userPhoneNumbers.TryGetValue(callbackQuery.From.Id, out var phone) ? phone : string.Empty;

                                // Запускаем автоматизацию поиска по номеру
                                _ = Task.Run(async () => await AutomateFindByNumberAsync(userDataDir, phoneNumber, cancellationToken));

                                // Списываем 1 оплаченный запуск (если это не бесплатное возобновление)
                                var skipCharge = _resumeFreeByUser.TryGetValue(callbackQuery.From.Id, out var resumedPhone) && !string.IsNullOrEmpty(resumedPhone) && _userPhoneNumbers.TryGetValue(callbackQuery.From.Id, out var currentPhone) && currentPhone == resumedPhone;
                                if (!skipCharge)
                                {
                                    try { await _supabaseService.TryConsumeOnePaidAccountAsync(callbackQuery.From.Id); } catch { }
                                }
                                _resumeFreeByUser.Remove(callbackQuery.From.Id);

                                // Стартуем 6-часовой прогрев для номера
                                var phoneForWarm = _userPhoneNumbers.TryGetValue(callbackQuery.From.Id, out var pfw) ? pfw : null;
                                if (!string.IsNullOrEmpty(phoneForWarm))
                                {
                                    StartWarmingTimer(phoneForWarm, chatId);
                                }

                                // Очищаем сессии после успешного входа
                                _verificationSessionDirByUser.Remove(callbackQuery.From.Id);
                                _userPhoneNumbers.Remove(callbackQuery.From.Id);
                            }
                            else
                            {
                                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Пока не вижу экран Чаты, попробуйте еще раз позже", cancellationToken: cancellationToken);
                            }
                        }
                        catch (Exception ex)
                        {
                            await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, $"Ошибка проверки: {ex.Message}", cancellationToken: cancellationToken);
                        }
                    });
                }
                else
                {
                    await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Сессия не найдена", cancellationToken: cancellationToken);
                }
                return;
            }

            // Если режим обслуживания включен, блокируем все действия кроме админа
            if (_maintenance && callbackQuery.From.Id != 1123842711)
            {
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "⏳ Бот на обслуживании. Попробуйте позже.", cancellationToken: cancellationToken);
                return;
            }

            switch (callbackQuery.Data)
            {
                case "profile":
                    // Получаем данные пользователя из базы данных
                    var user = await _supabaseService.GetOrCreateUserAsync(callbackQuery.From.Id, callbackQuery.From.Username ?? "Unknown");
                    
                    var profileMessage = $"👑 Профиль\n\n" +
                                       $"👍 Username: {user.Username}\n" +
                                       $"🔑 ID: {user.Id}\n" +
                                       $"$ Оплаченных аккаунтов: {user.PaidAccounts}\n" +
                                       $"📅 Дата регистрации: {user.RegistrationDate:dd.MM.yyyy HH:mm:ss}\n" +
                                       $"✨ Рефералов: {user.Referrals} шт";

                    var profileKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu")
                        }
                    });

                    await botClient.EditMessageTextAsync(
                        chatId: chatId,
                        messageId: messageId,
                        text: profileMessage,
                        replyMarkup: profileKeyboard,
                        cancellationToken: cancellationToken
                    );
                    break;

                case "cancel_auth":
                    // Обработка отмены авторизации
                    await HandleCancelAuthorizationAsync(botClient, callbackQuery, cancellationToken);
                    break;

                case "main_menu":
                    _adminActionState.Remove(callbackQuery.From.Id);
                    var welcomeMessage = $"Привет, {callbackQuery.From.Username}! 👋\n\n" +
                                       "➡ Atlantis Grev — бот для прогрева аккаунтов MAX\n\n" +
                                       "Чтобы добавить аккаунт, нажми на кнопку ➕ Добавить аккаунт.\n\n" +
                                       "❓ Чтобы ознакомиться с работой бота, нажмите Информацию.";

                    var keyboard = new InlineKeyboardMarkup(new[]
                    {
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("➕ Добавить аккаунт", "add_account"),
                            InlineKeyboardButton.WithCallbackData("💳 Оплатить", "pay")
                        },
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("👤 Профиль", "profile"),
                            InlineKeyboardButton.WithCallbackData("📱 Мои аккаунты", "my_accounts")
                        },
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("👥 Партнерская программа", "affiliate")
                        },
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("ℹ️ Информация", "info"),
                            InlineKeyboardButton.WithCallbackData("🛠️ Техподдержка", "support")
                        }
                    });

                    await botClient.EditMessageTextAsync(
                        chatId: chatId,
                        messageId: messageId,
                        text: welcomeMessage,
                        replyMarkup: keyboard,
                        cancellationToken: cancellationToken
                    );
                    break;

                case "info":
                    var allUsers = await _supabaseService.GetAllUsersAsync();
                    var totalAccounts = allUsers.Sum(u => u.PhoneNumbers?.Count ?? 0);
                    var activeWarming = _warmingCtsByPhone.Count;
                    var load = activeWarming switch
                    {
                        < 10 => "Низкая",
                        < 50 => "Средняя",
                        _ => "Высокая"
                    };

                    var infoMessage = "ℹ️ Информация о боте\n\n" +
                                      "Atlantis Grev — сервис для безопасного прогрева аккаунтов MAX.\n" +
                                      "Ниже отображается текущая статистика сервера.\n\n" +
                                      "------\n" +
                                      "Статистика сервера\n" +
                                      $"Всего аккаунтов: {totalAccounts}\n" +
                                      $"Прогревается сейчас: {activeWarming}\n" +
                                      $"Нагрузка: {load}";

                    var infoKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                    });

                    await botClient.EditMessageTextAsync(
                        chatId: chatId,
                        messageId: messageId,
                        text: infoMessage,
                        replyMarkup: infoKeyboard,
                        cancellationToken: cancellationToken
                    );
                    break;

                case "support":
                    if (_userActiveTicket.ContainsKey(callbackQuery.From.Id))
                    {
                        var tid = _userActiveTicket[callbackQuery.From.Id];
                        await botClient.EditMessageTextAsync(chatId, messageId, $"📝 У вас уже есть открытый тикет #{tid}. Напишите сообщение или /close для закрытия.", cancellationToken: cancellationToken);
                    }
                    else
                    {
                        _awaitingSupportCategory.Add(callbackQuery.From.Id);
                        var kb = new InlineKeyboardMarkup(new[]
                        {
                            new [] { InlineKeyboardButton.WithCallbackData("💳 Оплата", "support_cat_pay") },
                            new [] { InlineKeyboardButton.WithCallbackData("📦 Товары", "support_cat_goods") },
                            new [] { InlineKeyboardButton.WithCallbackData("❓ Другое", "support_cat_other") }
                        });
                        await botClient.EditMessageTextAsync(chatId, messageId, "Выберите категорию обращения:", replyMarkup: kb, cancellationToken: cancellationToken);
                    }
                    break;

                case "give_accounts":
                    if (callbackQuery.From.Id == 1123842711) // Проверка на админа
                    {
                        Console.WriteLine("Обрабатываю кнопку 'Выдать аккаунты'");
                        _adminActionState[callbackQuery.From.Id] = "give"; // Устанавливаем состояние
                        var giveAccountsMessage = "👤 Выдача аккаунтов\n\n" +
                                                "Введите ID пользователя и количество аккаунтов для прибавления:\n" +
                                                "`ID количество`\n\n" +
                                                "Например: `123456789 5` (прибавит 5 аккаунтов)\n\n" +
                                                "Или используйте команду: `/give ID количество`";

                        var giveAccountsKeyboard = new InlineKeyboardMarkup(new[]
                        {
                            new []
                            {
                                InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu")
                            }
                        });

                        try
                        {
                            await botClient.EditMessageTextAsync(
                                chatId: chatId,
                                messageId: messageId,
                                text: giveAccountsMessage,
                                replyMarkup: giveAccountsKeyboard,
                                cancellationToken: cancellationToken
                            );
                            Console.WriteLine("Сообщение 'Выдача аккаунтов' успешно отправлено");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Ошибка при отправке сообщения 'Выдача аккаунтов': {ex.Message}");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Пользователь {callbackQuery.From.Id} не является админом");
                    }
                    break;

                case "take_accounts":
                    if (callbackQuery.From.Id == 1123842711)
                    {
                        Console.WriteLine("Обрабатываю кнопку 'Убавить аккаунты'");
                        _adminActionState[callbackQuery.From.Id] = "take"; // Устанавливаем состояние
                        var takeMsg = "➖ Убавить оплаченные аккаунты\n\n" +
                                      "Введите ID пользователя и количество для вычитания:\n" +
                                      "`ID количество`\n\n" +
                                      "Например: `123456789 3` (убавит 3)";
                        var kb = new InlineKeyboardMarkup(new[]
                        {
                            new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                        });
                        try
                        {
                            await botClient.EditMessageTextAsync(chatId, messageId, takeMsg, replyMarkup: kb, cancellationToken: cancellationToken);
                        }
                        catch {}
                    }
                    break;

                case "give_time":
                    if (callbackQuery.From.Id == 1123842711)
                    {
                        var msg = "⏱️ Выдача времени\n\nВыберите вариант:";
                        var kb = new InlineKeyboardMarkup(new[]
                        {
                            new [] { InlineKeyboardButton.WithCallbackData("👥 Всем с номерами", "give_time_all") },
                            new [] { InlineKeyboardButton.WithCallbackData("👤 Одному пользователю", "give_time_user") },
                            new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                        });
                        await botClient.EditMessageTextAsync(chatId, messageId, msg, replyMarkup: kb, cancellationToken: cancellationToken);
                    }
                    break;

                case "give_time_all":
                    if (callbackQuery.From.Id == 1123842711)
                    {
                        _adminActionState[callbackQuery.From.Id] = "give_time_all";
                        var msg = "⏱️ Выдать время всем номерам пользователей, у которых есть хотя бы один добавленный аккаунт.\nВведите количество часов (1-48):";
                        var kb = new InlineKeyboardMarkup(new[]
                        {
                            new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                        });
                        await botClient.EditMessageTextAsync(chatId, messageId, msg, replyMarkup: kb, cancellationToken: cancellationToken);
                    }
                    break;

                case "give_time_user":
                    if (callbackQuery.From.Id == 1123842711)
                    {
                        _adminActionState[callbackQuery.From.Id] = "give_time_user";
                        var msg = "⏱️ Выдать время пользователю.\nВведите ID пользователя и количество часов через пробел (например `123456789 5`):";
                        var kb = new InlineKeyboardMarkup(new[]
                        {
                            new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                        });
                        await botClient.EditMessageTextAsync(chatId, messageId, msg, replyMarkup: kb, cancellationToken: cancellationToken);
                    }
                    break;

                case "toggle_maintenance":
                    if (callbackQuery.From.Id != 1123842711) break;
                    _maintenance = !_maintenance;
                    var stateText = _maintenance ? "Режим обслуживания включен. Пользователи временно не могут пользоваться ботом." : "Бот снова доступен пользователям.";
                    var maintenanceLabel2 = _maintenance ? "🟢 Включить бота" : "⛔ Поставить на паузу";
                    var adminKb2 = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithCallbackData("👤 Выдать аккаунты", "give_accounts"), InlineKeyboardButton.WithCallbackData("⏱️ Выдать время", "give_time"), InlineKeyboardButton.WithCallbackData("📊 Статистика", "admin_stats") },
                        new [] { InlineKeyboardButton.WithCallbackData("👥 Управление рефералами", "manage_referrals"), InlineKeyboardButton.WithCallbackData("⚙️ Настройки", "admin_settings") },
                        new [] { InlineKeyboardButton.WithCallbackData(maintenanceLabel2, "toggle_maintenance") },
                        new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                    });
                    await botClient.EditMessageTextAsync(chatId, messageId, "🔧 " + stateText, replyMarkup: adminKb2, cancellationToken: cancellationToken);
                    break;

                case "admin_stats":
                    if (callbackQuery.From.Id == 1123842711) // Проверка на админа
                    {
                        Console.WriteLine("Обрабатываю кнопку 'Статистика'");
                        var statsMessage = "📊 Статистика\n\n" +
                                         "Общее количество пользователей: [будет добавлено]\n" +
                                         "Всего оплаченных аккаунтов: [будет добавлено]\n" +
                                         "Всего рефералов: [будет добавлено]";

                        var statsKeyboard = new InlineKeyboardMarkup(new[]
                        {
                            new []
                            {
                                InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu")
                            }
                        });

                        try
                        {
                            await botClient.EditMessageTextAsync(
                                chatId: chatId,
                                messageId: messageId,
                                text: statsMessage,
                                replyMarkup: statsKeyboard,
                                cancellationToken: cancellationToken
                            );
                            Console.WriteLine("Сообщение 'Статистика' успешно отправлено");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Ошибка при отправке сообщения 'Статистика': {ex.Message}");
                        }
                    }
                    break;

                case "manage_referrals":
                    if (callbackQuery.From.Id == 1123842711) // Проверка на админа
                    {
                        Console.WriteLine("Обрабатываю кнопку 'Управление рефералами'");
                        var referralsMessage = "👥 Управление рефералами\n\n" +
                                             "Функция в разработке...";

                        var referralsKeyboard = new InlineKeyboardMarkup(new[]
                        {
                            new []
                            {
                                InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu")
                            }
                        });

                        try
                        {
                            await botClient.EditMessageTextAsync(
                                chatId: chatId,
                                messageId: messageId,
                                text: referralsMessage,
                                replyMarkup: referralsKeyboard,
                                cancellationToken: cancellationToken
                            );
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Ошибка при отправке сообщения 'Управление рефералами': {ex.Message}");
                        }
                    }
                    break;

                case "admin_broadcast_copy":
                    if (callbackQuery.From.Id != 1123842711)
                    {
                        await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Нет доступа", cancellationToken: cancellationToken);
                        return;
                    }
                    _awaitingBroadcastMode = BroadcastMode.Copy;
                    await botClient.EditMessageTextAsync(chatId, messageId,
                        "📢 Режим рассылки: копирование сообщения.\n\nПришлите следующее сообщение (текст/фото/видео/документ/голос/стикер) — я скопирую его всем пользователям.\n\nЧтобы отменить: /cancel_broadcast",
                        cancellationToken: cancellationToken);
                    return;

                case "admin_broadcast_forward":
                    if (callbackQuery.From.Id != 1123842711)
                    {
                        await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Нет доступа", cancellationToken: cancellationToken);
                        return;
                    }
                    _awaitingBroadcastMode = BroadcastMode.Forward;
                    await botClient.EditMessageTextAsync(chatId, messageId,
                        "🔁 Режим рассылки: пересылка сообщения.\n\nПерешлите следующее сообщение — я перешлю его всем пользователям с указанием источника.\n\nЧтобы отменить: /cancel_broadcast",
                        cancellationToken: cancellationToken);
                    return;

                case "admin_settings":
                    if (callbackQuery.From.Id == 1123842711) // Проверка на админа
                    {
                        Console.WriteLine("Обрабатываю кнопку 'Настройки'");
                        var settingsMessage = "⚙️ Настройки\n\n" +
                                            "Функция в разработке...";

                        var settingsKeyboard = new InlineKeyboardMarkup(new[]
                        {
                            new []
                            {
                                InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu")
                            }
                        });

                        try
                        {
                            await botClient.EditMessageTextAsync(
                                chatId: chatId,
                                messageId: messageId,
                                text: settingsMessage,
                                replyMarkup: settingsKeyboard,
                                cancellationToken: cancellationToken
                            );
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Ошибка при отправке сообщения 'Настройки': {ex.Message}");
                        }
                    }
                    break;

                case "my_accounts":
                    Console.WriteLine("Обрабатываю кнопку 'Мои аккаунты'");
                    var accountsUser = await _supabaseService.GetUserAsync(callbackQuery.From.Id);
                    if (accountsUser != null)
                    {
                        var accountsMessage = "📱 Мои аккаунты\n\n";
                        
                        						if (accountsUser.PhoneNumbers != null && accountsUser.PhoneNumbers.Count > 0)
						{
							// Ничего в тексте не выводим, список будет кнопками
						}
						else
						{
							accountsMessage += "Список пуст.\n";
						}
						
						var rows = new List<InlineKeyboardButton[]>();
						if (accountsUser.PhoneNumbers != null)
						{
							foreach (var ph in accountsUser.PhoneNumbers)
							{
								rows.Add(new [] { InlineKeyboardButton.WithCallbackData(ph, $"acc:{ph}") });
							}
						}
						rows.Add(new [] { InlineKeyboardButton.WithCallbackData("Добавить аккаунт 📞", "add_account") });
						rows.Add(new [] { InlineKeyboardButton.WithCallbackData("← Меню", "main_menu") });
						var accountsKeyboard = new InlineKeyboardMarkup(rows.ToArray());

                        try
                        {
                            await botClient.EditMessageTextAsync(
                                chatId: chatId,
                                messageId: messageId,
                                text: accountsMessage,
                                replyMarkup: accountsKeyboard,
                                cancellationToken: cancellationToken
                            );
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Ошибка при отправке сообщения 'Мои аккаунты': {ex.Message}");
                        }
                    }
                    break;

                				case "add_account":
					Console.WriteLine("Обрабатываю кнопку 'Добавить аккаунт'");
                    var addAccountMessage = "➕ Добавление аккаунта\n\n" +
                                          "Введите номер телефона в формате:\n" +
                                          "`+79001234567`\n\n" +
                                          "Или в любом другом удобном формате.";

                    var addAccountKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("🔙 Назад", "my_accounts")
                        }
                    });

                    try
                    {
                        await botClient.EditMessageTextAsync(
                            chatId: chatId,
                            messageId: messageId,
                            text: addAccountMessage,
                            replyMarkup: addAccountKeyboard,
                            cancellationToken: cancellationToken
                        );
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Ошибка при отправке сообщения 'Добавить аккаунт': {ex.Message}");
                    }
                    break;

                case "pay":
                    Console.WriteLine("Обрабатываю кнопку 'Оплатить'");
                    var payMessage = "💳 Оплата\n\n" +
                                     "Сколько аккаунтов хотите оплатить? (от 1 до 100)\n\n" +
                                     $"Цена одного аккаунта: {PricePerAccountUsdt:F2} USDT";

                    var payKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                    });

                    try
                    {
                        await botClient.EditMessageTextAsync(
                            chatId: chatId,
                            messageId: messageId,
                            text: payMessage,
                            replyMarkup: payKeyboard,
                            cancellationToken: cancellationToken
                        );
                        _awaitingPaymentQtyUserIds.Add(callbackQuery.From.Id);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Ошибка при отправке сообщения 'Оплата': {ex.Message}");
                    }
                    break;

                case "affiliate":
                    await HandleAffiliateProgramAsync(botClient, callbackQuery, cancellationToken);
                    break;
                case "affiliate_stats":
                    await HandleAffiliateStatsAsync(botClient, callbackQuery, cancellationToken);
                    break;
                case "affiliate_referrals":
                    await HandleAffiliateReferralsAsync(botClient, callbackQuery, cancellationToken);
                    break;
                case "affiliate_withdraw":
                    await HandleAffiliateWithdrawAsync(botClient, callbackQuery, cancellationToken);
                    break;
                case "affiliate_history":
                    await HandleAffiliateHistoryAsync(botClient, callbackQuery, cancellationToken);
                    break;
            }
        }

        private static Task HandlePollingErrorAsync(ITelegramBotClient botClient, Exception exception, CancellationToken cancellationToken)
        {
            var ErrorMessage = exception switch
            {
                ApiRequestException apiRequestException
                    => $"Telegram API Error:\n{apiRequestException.ErrorCode}\n{apiRequestException.Message}",
                _ => exception.ToString()
            };

            Console.WriteLine(ErrorMessage);
            return Task.CompletedTask;
        }

        private static async Task HandleCancelAuthorizationAsync(ITelegramBotClient botClient, CallbackQuery callbackQuery, CancellationToken cancellationToken)
        {
            var chatId = callbackQuery.Message.Chat.Id;
            var messageId = callbackQuery.Message.MessageId;
            var userId = callbackQuery.From.Id;

            try
            {
                // Отвечаем на callback query
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "⏹️ Отмена авторизации...", cancellationToken: cancellationToken);

                // Получаем номер телефона из активной сессии
                string phoneNumber = "неизвестный номер";
                
                // Ищем активную сессию пользователя
                string? userDataDir = null;
                if (_awaitingCodeSessionDirByUser.TryGetValue(userId, out var dirAwait))
                {
                    userDataDir = dirAwait;
                    CancelAuthorizationTimeout(userId);
                    _awaitingCodeSessionDirByUser.Remove(userId);
                }
                else if (_lastSessionDirByUser.TryGetValue(userId, out var dirLast))
                {
                    userDataDir = dirLast;
                    _lastSessionDirByUser.Remove(userId);
                }

                if (userDataDir != null)
                {
                    if (_userPhoneNumbers.TryGetValue(userId, out var savedPhone))
                    {
                        phoneNumber = savedPhone;
                        _userPhoneNumbers.Remove(userId);
                        _sessionDirByPhone.Remove(phoneNumber);
                    }

                    try
                    {
                        await using var cdp = await MaxWebAutomation.ConnectAsync(userDataDir, "web");
                        await cdp.CloseBrowserAsync();
                    }
                    catch
                    {
                    }
                }
                else
                {
                    await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Нет активной авторизации", cancellationToken: cancellationToken);
                    return;
                }

                // Отправляем сообщение об отмене
                var cancelMessage = $"⏹️ **Авторизация отменена!**\n\n" +
                                   $"📱 Номер: `{phoneNumber}`\n\n" +
                                   $"✅ Вы можете:\n" +
                                   $"• Запустить авторизацию заново\n" +
                                   $"• Использовать другой номер\n" +
                                   $"• Обратиться в поддержку\n\n" +
                                   $"🔙 Для возврата в главное меню нажмите кнопку ниже.";

                var cancelKeyboard = new InlineKeyboardMarkup(new[]
                {
                    new []
                    {
                        InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu")
                    }
                });

                await botClient.EditMessageTextAsync(
                    chatId: chatId,
                    messageId: messageId,
                    text: cancelMessage,
                    replyMarkup: cancelKeyboard,
                    cancellationToken: cancellationToken
                );

                Console.WriteLine($"[MAX] Пользователь {userId} отменил авторизацию для номера {phoneNumber}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MAX] Ошибка при отмене авторизации: {ex.Message}");
                
                // Отправляем простое сообщение об ошибке
                try
                {
                    await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "❌ Ошибка при отмене", cancellationToken: cancellationToken);
                }
                catch {}
            }
        }

        private static async Task HandleDeleteAccountAsync(ITelegramBotClient botClient, CallbackQuery callbackQuery, string phoneNumber, CancellationToken cancellationToken)
        {
            var chatId = callbackQuery.Message.Chat.Id;
            var messageId = callbackQuery.Message.MessageId;
            var userId = callbackQuery.From.Id;

            try
            {
                // Отвечаем на callback query
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "🗑️ Удаление аккаунта...", cancellationToken: cancellationToken);

                // Удаляем номер из базы данных
                var success = await _supabaseService.RemovePhoneNumberAsync(userId, phoneNumber);
                
                if (success)
                {
                    // Останавливаем прогрев и очищаем состояние для удаляемого номера
                    if (_warmingCtsByPhone.TryGetValue(phoneNumber, out var cts))
                    {
                        try { cts.Cancel(); } catch { }
                        _warmingCtsByPhone.Remove(phoneNumber);
                    }
                    _warmingEndsByPhone.Remove(phoneNumber);
                    _warmingRemainingByPhone.Remove(phoneNumber);
                    SaveWarmingState();
                    _sessionDirByPhone.Remove(phoneNumber);
                    _lastUsedNumberByUser.Remove(callbackQuery.From.Id); // Очищаем последний использованный номер

                    // Успешное удаление
                    var successMessage = $"✅ **Аккаунт удален!**\n\n" +
                                        $"📱 Номер: `{phoneNumber}`\n\n" +
                                        $"🗑️ Номер успешно удален из ваших аккаунтов.\n\n" +
                                        $"📋 Вы можете:\n" +
                                        $"• Добавить новый аккаунт\n" +
                                        $"• Просмотреть оставшиеся аккаунты\n" +
                                        $"• Вернуться в главное меню";

                    var successKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("📱 Мои аккаунты", "my_accounts"),
                            InlineKeyboardButton.WithCallbackData("➕ Добавить аккаунт", "add_account")
                        },
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu")
                        }
                    });

                    await botClient.EditMessageTextAsync(
                        chatId: chatId,
                        messageId: messageId,
                        text: successMessage,
                        replyMarkup: successKeyboard,
                        cancellationToken: cancellationToken
                    );

                    Console.WriteLine($"[DELETE] Пользователь {userId} удалил аккаунт {phoneNumber}");
                }
                else
                {
                    // Ошибка удаления
                    var errorMessage = $"❌ **Ошибка удаления!**\n\n" +
                                      $"📱 Номер: `{phoneNumber}`\n\n" +
                                      $"⚠️ Не удалось удалить номер из ваших аккаунтов.\n\n" +
                                      $"🔧 Возможные причины:\n" +
                                      $"• Номер не найден в ваших аккаунтах\n" +
                                      $"• Проблемы с базой данных\n" +
                                      $"• Ошибка сети\n\n" +
                                      $"🔄 Попробуйте еще раз или обратитесь в поддержку.";

                    var errorKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("🔄 Попробовать снова", $"delete_account:{phoneNumber}"),
                            InlineKeyboardButton.WithCallbackData("📱 Мои аккаунты", "my_accounts")
                        },
                        new []
                        {
                            InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu")
                        }
                    });

                    await botClient.EditMessageTextAsync(
                        chatId: chatId,
                        messageId: messageId,
                        text: errorMessage,
                        replyMarkup: errorKeyboard,
                        cancellationToken: cancellationToken
                    );

                    Console.WriteLine($"[DELETE] Ошибка удаления аккаунта {phoneNumber} пользователем {userId}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DELETE] Ошибка при удалении аккаунта: {ex.Message}");
                
                // Отправляем сообщение об ошибке
                var errorMessage = $"❌ **Критическая ошибка!**\n\n" +
                                  $"📱 Номер: `{phoneNumber}`\n\n" +
                                  $"💥 Произошла непредвиденная ошибка при удалении аккаунта.\n\n" +
                                  $"🔧 Ошибка: `{ex.Message}`\n\n" +
                                  $"📞 Обратитесь в поддержку для решения проблемы.";

                var errorKeyboard = new InlineKeyboardMarkup(new[]
                {
                    new []
                    {
                        InlineKeyboardButton.WithCallbackData("📱 Мои аккаунты", "my_accounts"),
                        InlineKeyboardButton.WithCallbackData("🛠️ Техподдержка", "support")
                    },
                    new []
                    {
                        InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu")
                    }
                });

                await botClient.EditMessageTextAsync(
                    chatId: chatId,
                    messageId: messageId,
                    text: errorMessage,
                    replyMarkup: errorKeyboard,
                    cancellationToken: cancellationToken
                );
            }
        }

        // Обработчик партнерской программы
        private static async Task HandleAffiliateProgramAsync(ITelegramBotClient botClient, CallbackQuery callbackQuery, CancellationToken cancellationToken)
        {
            var chatId = callbackQuery.Message?.Chat.Id;
            var messageId = callbackQuery.Message?.MessageId;
            var userId = callbackQuery.From?.Id;

            if (chatId == null || messageId == null || userId == null)
            {
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Ошибка получения данных", cancellationToken: cancellationToken);
                return;
            }

            try
            {
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, cancellationToken: cancellationToken);
                Console.WriteLine($"[AFFILIATE] Пользователь {userId} открыл партнерскую программу");

                // Получаем данные пользователя
                var user = await _supabaseService.GetUserAsync(userId.Value);
                if (user == null)
                {
                    await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, "❌ Ошибка загрузки данных пользователя", cancellationToken: cancellationToken);
                    return;
                }

                // Получаем или создаем affiliate пользователя
                var affiliateUser = await _supabaseService.GetAffiliateUserAsync(userId.Value);
                if (affiliateUser == null)
                {
                    // Создаем affiliate пользователя
                    var newAffiliateCode = await _supabaseService.GenerateAffiliateCodeAsync(userId.Value);
                    affiliateUser = await _supabaseService.GetAffiliateUserAsync(userId.Value);
                }

                // Используем данные из affiliate_users или временные значения
                var affiliateCode = affiliateUser?.AffiliateCode ?? $"REF{userId}";
                var affiliateBalance = affiliateUser?.AffiliateBalance ?? 0;

                // Получаем статистику рефералов
                var referrals = await _supabaseService.GetUserReferralsAsync(userId.Value);
                var earnings = await _supabaseService.GetUserEarningsAsync(userId.Value);

                // Рассчитываем статистику
                var totalEarned = earnings.Sum(e => e.AmountUsdt);

                var affiliateMessage = $"👥 **Партнерская программа**\n\n" +
                                     $"💰 **Ваш баланс:** {affiliateBalance:F2} USDT\n" +
                                     $"📈 **Всего заработано:** {totalEarned:F2} USDT\n\n" +
                                     $"👥 **Рефералы:** {referrals.Count} человек\n" +
                                     $"📊 **Активные рефералы:** {referrals.Count(r => r.PaidAccounts > 0)} человек\n\n" +
                                     $"🔗 **Ваша реферальная ссылка:**\n" +
                                     $"`https://t.me/AtlantisGrevMAX_bot?start=ref{affiliateCode}`\n\n" +
                                     $"💡 **Как заработать:**\n" +
                                     $"• {ReferralPaymentCommission * 100:F0}% с каждого платежа реферала\n" +
                                     $"• Минимум для вывода: {MinimumWithdrawal:F2} USDT";

                var keyboard = new InlineKeyboardMarkup(new[]
                {
                    new [] { InlineKeyboardButton.WithCallbackData("📊 Статистика", "affiliate_stats"), InlineKeyboardButton.WithCallbackData("👥 Мои рефералы", "affiliate_referrals") },
                    new [] { InlineKeyboardButton.WithCallbackData("💰 Вывод средств", "affiliate_withdraw"), InlineKeyboardButton.WithCallbackData("📋 История выводов", "affiliate_history") },
                    new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                });

                await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, affiliateMessage, replyMarkup: keyboard, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка обработки партнерской программы: {ex.Message}");
                await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, "❌ Ошибка загрузки партнерской программы", cancellationToken: cancellationToken);
            }
        }

        // Обработчик вывода средств из партнерской программы
        private static async Task HandleAffiliateWithdrawAsync(ITelegramBotClient botClient, CallbackQuery callbackQuery, CancellationToken cancellationToken)
        {
            var chatId = callbackQuery.Message?.Chat.Id;
            var messageId = callbackQuery.Message?.MessageId;
            var userId = callbackQuery.From?.Id;

            if (chatId == null || messageId == null || userId == null)
            {
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Ошибка получения данных", cancellationToken: cancellationToken);
                return;
            }

            try
            {
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, cancellationToken: cancellationToken);

                // Получаем данные пользователя
                var user = await _supabaseService.GetUserAsync(userId.Value);
                if (user == null)
                {
                    await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, "❌ Ошибка загрузки данных пользователя", cancellationToken: cancellationToken);
                    return;
                }

                // Получаем affiliate данные
                var affiliateUser = await _supabaseService.GetAffiliateUserAsync(userId.Value);
                if (affiliateUser == null)
                {
                    await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, "❌ Ошибка загрузки данных партнерской программы", cancellationToken: cancellationToken);
                    return;
                }

                // Проверяем баланс
                if (affiliateUser.AffiliateBalance < MinimumWithdrawal)
                {
                    var errorMessage = $"❌ **Недостаточно средств для вывода!**\n\n" +
                                     $"💰 Ваш баланс: {affiliateUser.AffiliateBalance:F2} USDT\n" +
                                     $"📊 Минимум для вывода: {MinimumWithdrawal:F2} USDT\n\n" +
                                     $"💡 Приглашайте больше рефералов для заработка!";

                    var errorKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithCallbackData("👥 Партнерская программа", "affiliate") },
                        new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                    });

                    await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, errorMessage, replyMarkup: errorKeyboard, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
                    return;
                }

                // Проверяем баланс бота
                var botBalance = await _cryptoPayService.GetBalanceAsync("USDT");
                if (botBalance < affiliateUser.AffiliateBalance)
                {
                    var errorMessage = $"❌ **Временно недоступно!**\n\n" +
                                     $"💰 Ваш баланс: {affiliateUser.AffiliateBalance:F2} USDT\n" +
                                     $"🤖 Баланс бота: {botBalance:F2} USDT\n\n" +
                                     $"⏳ Попробуйте позже или обратитесь в поддержку.";

                    var errorKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithCallbackData("👥 Партнерская программа", "affiliate") },
                        new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                    });

                    await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, errorMessage, replyMarkup: errorKeyboard, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
                    return;
                }

                // Сохраняем сумму к выводу до обнуления баланса
                var amountToWithdraw = affiliateUser.AffiliateBalance;

                // Создаем чек для выплаты
                var check = await _cryptoPayService.CreateCheckAsync(
                    amountToWithdraw,
                    "USDT",
                    $"Выплата партнерской программы пользователю {user.Username}"
                );

                if (check == null)
                {
                    await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, "❌ Ошибка создания чека. Попробуйте позже.", cancellationToken: cancellationToken);
                    return;
                }

                // Обновляем баланс пользователя (обнуляем)
                await _supabaseService.UpdateUserAffiliateBalanceAsync(userId.Value, 0, affiliateUser.TotalEarned);

                // Создаем запись о выводе средств в истории
                var withdrawalLogged = await _supabaseService.CreateWithdrawalRequestAsync(
                    userId.Value,
                    amountToWithdraw,
                    "Crypto Pay Check",
                    "CRYPTO_PAY"
                );

                if (!withdrawalLogged)
                {
                    await botClient.EditMessageTextAsync(chatId.Value, messageId.Value,
                        "❌ Ошибка сохранения истории вывода. Попробуйте позже.",
                        cancellationToken: cancellationToken);
                    return;
                }

                // Отправляем сообщение с чеком
                var successMessage = $"✅ **Выплата успешно создана!**\n\n" +
                                   $"💰 Сумма: {amountToWithdraw:F2} USDT\n" +
                                   $"📅 Дата: {DateTime.Now:dd.MM.yyyy HH:mm}\n" +
                                   $"🆔 ID чека: {check.CheckId}\n\n" +
                                   $"🔗 **Ваш чек:**\n" +
                                   $"{check.BotCheckUrl}\n\n" +
                                   $"💡 Нажмите на ссылку выше, чтобы получить средства!";

                var successKeyboard = new InlineKeyboardMarkup(new[]
                {
                    new [] { InlineKeyboardButton.WithUrl("💰 Получить средства", check.BotCheckUrl) },
                    new [] { InlineKeyboardButton.WithCallbackData("👥 Партнерская программа", "affiliate") },
                    new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                });

                await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, successMessage, replyMarkup: successKeyboard, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);

                Console.WriteLine($"[AFFILIATE] ✅ Выплата создана для {user.Username}: {amountToWithdraw:F2} USDT (чек: {check.CheckId})");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка обработки вывода: {ex.Message}");
                await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, "❌ Ошибка обработки вывода. Попробуйте позже.", cancellationToken: cancellationToken);
            }
        }

        // Обработчик просмотра статистики партнерской программы
        private static async Task HandleAffiliateStatsAsync(ITelegramBotClient botClient, CallbackQuery callbackQuery, CancellationToken cancellationToken)
        {
            var chatId = callbackQuery.Message?.Chat.Id;
            var messageId = callbackQuery.Message?.MessageId;
            var userId = callbackQuery.From?.Id;

            if (chatId == null || messageId == null || userId == null)
            {
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Ошибка получения данных", cancellationToken: cancellationToken);
                return;
            }

            try
            {
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, cancellationToken: cancellationToken);

                // Загружаем данные о пользователе и его рефералах
                var affiliateUser = await _supabaseService.GetAffiliateUserAsync(userId.Value);
                var referrals = await _supabaseService.GetUserReferralsAsync(userId.Value);
                var earnings = await _supabaseService.GetUserEarningsAsync(userId.Value);

                var totalEarned = earnings.Sum(e => e.AmountUsdt);
                var paid = earnings.Where(e => e.Status != "pending").Sum(e => e.AmountUsdt);

                var statsMessage = $"📊 **Статистика партнерской программы**\n\n" +
                                   $"👥 Всего рефералов: {referrals.Count}\n" +
                                   $"🔥 Активных: {referrals.Count(r => r.PaidAccounts > 0)}\n\n" +
                                   $"💰 Баланс: {affiliateUser?.AffiliateBalance ?? 0:F2} USDT\n" +
                                   $"💸 Всего заработано: {totalEarned:F2} USDT\n" +
                                   $"✅ Выплачено: {paid:F2} USDT";

                var keyboard = new InlineKeyboardMarkup(new[]
                {
                    new [] { InlineKeyboardButton.WithCallbackData("👥 Партнерская программа", "affiliate"), InlineKeyboardButton.WithCallbackData("👥 Мои рефералы", "affiliate_referrals") },
                    new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                });

                await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, statsMessage, replyMarkup: keyboard, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка загрузки статистики: {ex.Message}");
                await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, "❌ Ошибка загрузки статистики.", cancellationToken: cancellationToken);
            }
        }

        // Обработчик просмотра списка рефералов
        private static async Task HandleAffiliateReferralsAsync(ITelegramBotClient botClient, CallbackQuery callbackQuery, CancellationToken cancellationToken)
        {
            var chatId = callbackQuery.Message?.Chat.Id;
            var messageId = callbackQuery.Message?.MessageId;
            var userId = callbackQuery.From?.Id;

            if (chatId == null || messageId == null || userId == null)
            {
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Ошибка получения данных", cancellationToken: cancellationToken);
                return;
            }

            try
            {
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, cancellationToken: cancellationToken);

                var referrals = await _supabaseService.GetUserReferralsAsync(userId.Value);

                if (referrals.Count == 0)
                {
                    var emptyMessage = $"👥 **Мои рефералы**\n\nУ вас пока нет рефералов.";
                    var emptyKb = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithCallbackData("👥 Партнерская программа", "affiliate"), InlineKeyboardButton.WithCallbackData("📊 Статистика", "affiliate_stats") },
                        new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                    });

                    await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, emptyMessage, replyMarkup: emptyKb, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
                    return;
                }

                var referralsMessage = new StringBuilder();
                referralsMessage.AppendLine("👥 **Мои рефералы**\n");

                int i = 1;
                foreach (var r in referrals.OrderByDescending(r => r.RegistrationDate).Take(10))
                {
                    var name = string.IsNullOrEmpty(r.Username) ? $"ID:{r.Id}" : $"@{r.Username}";
                    var state = r.PaidAccounts > 0 ? "активен" : "не активен";
                    referralsMessage.AppendLine($"{i}. {name} — {state} (с {r.RegistrationDate:dd.MM.yyyy})");
                    i++;
                }

                referralsMessage.AppendLine();
                referralsMessage.AppendLine($"Всего: {referrals.Count}, активных: {referrals.Count(r => r.PaidAccounts > 0)}");

                var kb = new InlineKeyboardMarkup(new[]
                {
                    new [] { InlineKeyboardButton.WithCallbackData("📊 Статистика", "affiliate_stats"), InlineKeyboardButton.WithCallbackData("👥 Партнерская программа", "affiliate") },
                    new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                });

                await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, referralsMessage.ToString(), replyMarkup: kb, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка загрузки рефералов: {ex.Message}");
                await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, "❌ Ошибка загрузки списка рефералов.", cancellationToken: cancellationToken);
            }
        }

        // Обработчик истории выводов
        private static async Task HandleAffiliateHistoryAsync(ITelegramBotClient botClient, CallbackQuery callbackQuery, CancellationToken cancellationToken)
        {
            var chatId = callbackQuery.Message?.Chat.Id;
            var messageId = callbackQuery.Message?.MessageId;
            var userId = callbackQuery.From?.Id;

            if (chatId == null || messageId == null || userId == null)
            {
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, "Ошибка получения данных", cancellationToken: cancellationToken);
                return;
            }

            try
            {
                await botClient.AnswerCallbackQueryAsync(callbackQuery.Id, cancellationToken: cancellationToken);

                // Получаем историю выводов
                var withdrawals = await _supabaseService.GetUserWithdrawalsAsync(userId.Value);
                
                if (withdrawals.Count == 0)
                {
                    var noHistoryMessage = $"📋 **История выводов**\n\n" +
                                         $"У вас пока нет заявок на вывод средств.\n\n" +
                                         $"💡 Заработайте средства в партнерской программе!";

                    var noHistoryKeyboard = new InlineKeyboardMarkup(new[]
                    {
                        new [] { InlineKeyboardButton.WithCallbackData("👥 Партнерская программа", "affiliate") },
                        new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                    });

                    await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, noHistoryMessage, replyMarkup: noHistoryKeyboard, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
                    return;
                }

                var historyMessage = $"📋 **История выводов**\n\n";
                var totalWithdrawn = 0m;

                foreach (var withdrawal in withdrawals.Take(10)) // Показываем последние 10
                {
                    var status = withdrawal.Status switch
                    {
                        "pending" => "⏳ Ожидает",
                        "processing" => "🔄 Обрабатывается",
                        "completed" => "✅ Выполнен",
                        "rejected" => "❌ Отклонен",
                        _ => "❓ Неизвестно"
                    };

                    historyMessage += $"💰 **{withdrawal.AmountUsdt:F2} USDT** ({withdrawal.Network})\n" +
                                      $"📅 {withdrawal.CreatedAt:dd.MM.yyyy HH:mm}\n" +
                                      $"👛 {withdrawal.WalletAddress}\n" +
                                      $"📊 Статус: {status}\n";

                    if (withdrawal.Status == "completed" && withdrawal.ProcessedAt.HasValue)
                        historyMessage += $"✅ Выплачено: {withdrawal.ProcessedAt.Value:dd.MM.yyyy HH:mm}\n";

                    historyMessage += "\n";

                    if (withdrawal.Status == "completed")
                        totalWithdrawn += withdrawal.AmountUsdt;
                }

                historyMessage += $"📈 **Всего выведено:** {totalWithdrawn:F2} USDT";

                var historyKeyboard = new InlineKeyboardMarkup(new[]
                {
                    new [] { InlineKeyboardButton.WithCallbackData("👥 Партнерская программа", "affiliate") },
                    new [] { InlineKeyboardButton.WithCallbackData("🏠 Главное меню", "main_menu") }
                });

                await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, historyMessage, replyMarkup: historyKeyboard, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AFFILIATE] Ошибка загрузки истории: {ex.Message}");
                await botClient.EditMessageTextAsync(chatId.Value, messageId.Value, "❌ Ошибка загрузки истории. Попробуйте позже.", cancellationToken: cancellationToken);
            }
        }

        private static async Task RunBroadcastAsync(ITelegramBotClient botClient, Message sourceMessage, BroadcastMode mode, CancellationToken cancellationToken)
        {
            try
            {
                var adminChatId = sourceMessage.Chat.Id;
                await botClient.SendTextMessageAsync(adminChatId, "📥 Получил сообщение для рассылки. Формирую список пользователей...", cancellationToken: cancellationToken);

                var userIds = await _supabaseService.GetAllUserIdsAsync();
                userIds = userIds.Where(id => id != 1123842711).Distinct().ToList(); // исключаем админа

                if (userIds.Count == 0)
                {
                    await botClient.SendTextMessageAsync(adminChatId, "⚠️ Нет пользователей для рассылки.", cancellationToken: cancellationToken);
                    return;
                }

                await botClient.SendTextMessageAsync(adminChatId, $"👥 Пользователей для рассылки: {userIds.Count}", cancellationToken: cancellationToken);

                int success = 0, failed = 0;
                int batch = 0;
                var sw = Stopwatch.StartNew();

                foreach (var uid in userIds)
                {
                    // Проверяем отмену операции
                    if (cancellationToken.IsCancellationRequested)
                    {
                        await botClient.SendTextMessageAsync(adminChatId, "❌ Рассылка отменена.", cancellationToken: cancellationToken);
                        return;
                    }
                    
                    try
                    {
                        // Троттлинг, чтобы не упереться в лимиты Telegram
                        if (batch++ % 25 == 0)
                        {
                            await Task.Delay(1000, cancellationToken);
                        }

                        if (mode == BroadcastMode.Forward)
                        {
                            await botClient.ForwardMessageAsync(uid, sourceMessage.Chat.Id, sourceMessage.MessageId, cancellationToken: cancellationToken);
                        }
                        else
                        {
                            // Копируем тип сообщения
                            switch (sourceMessage.Type)
                            {
                                case MessageType.Text:
                                    await botClient.SendTextMessageAsync(uid, sourceMessage.Text, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
                                    break;
                                case MessageType.Photo:
                                    var ph = sourceMessage.Photo?.OrderBy(p => p.FileSize).LastOrDefault();
                                    if (ph != null)
                                    {
                                        await botClient.SendPhotoAsync(uid, InputFile.FromFileId(ph.FileId), caption: sourceMessage.Caption, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
                                    }
                                    break;
                                case MessageType.Video:
                                    if (sourceMessage.Video != null)
                                    {
                                        await botClient.SendVideoAsync(uid, InputFile.FromFileId(sourceMessage.Video.FileId), caption: sourceMessage.Caption, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
                                    }
                                    break;
                                case MessageType.Document:
                                    if (sourceMessage.Document != null)
                                    {
                                        await botClient.SendDocumentAsync(uid, InputFile.FromFileId(sourceMessage.Document.FileId), caption: sourceMessage.Caption, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
                                    }
                                    break;
                                case MessageType.Audio:
                                    if (sourceMessage.Audio != null)
                                    {
                                        await botClient.SendAudioAsync(uid, InputFile.FromFileId(sourceMessage.Audio.FileId), caption: sourceMessage.Caption, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
                                    }
                                    break;
                                case MessageType.Voice:
                                    if (sourceMessage.Voice != null)
                                    {
                                        await botClient.SendVoiceAsync(uid, InputFile.FromFileId(sourceMessage.Voice.FileId), caption: sourceMessage.Caption, cancellationToken: cancellationToken);
                                    }
                                    break;
                                case MessageType.Sticker:
                                    if (sourceMessage.Sticker != null)
                                    {
                                        await botClient.SendStickerAsync(uid, InputFile.FromFileId(sourceMessage.Sticker.FileId), cancellationToken: cancellationToken);
                                    }
                                    break;
                                case MessageType.Animation:
                                    if (sourceMessage.Animation != null)
                                    {
                                        await botClient.SendAnimationAsync(uid, InputFile.FromFileId(sourceMessage.Animation.FileId), caption: sourceMessage.Caption, parseMode: ParseMode.Markdown, cancellationToken: cancellationToken);
                                    }
                                    break;
                                default:
                                    await botClient.SendTextMessageAsync(uid, sourceMessage.Text ?? "", cancellationToken: cancellationToken);
                                    break;
                            }
                        }

                        success++;
                    }
                    catch (Exception ex)
                    {
                        failed++;
                        Console.WriteLine($"[BROADCAST] Ошибка отправки пользователю {uid}: {ex.Message}");
                        // Игнорируем индивидуальные ошибки и идем дальше
                    }
                }

                sw.Stop();
                await botClient.SendTextMessageAsync(adminChatId,
                    $"✅ Рассылка завершена за {sw.Elapsed.TotalSeconds:F1}с.\n\n" +
                    $"📬 Успешно: {success}\n" +
                    $"⚠️ Ошибок: {failed}", cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BROADCAST] Критическая ошибка рассылки: {ex.Message}");
                await botClient.SendTextMessageAsync(sourceMessage.Chat.Id, $"❌ Ошибка рассылки: {ex.Message}", cancellationToken: cancellationToken);
            }
        }

        private static async Task<bool> CheckChatsScreenAsync(MaxWebAutomation cdp, int totalTimeoutMs = 30000, int pollMs = 300)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < totalTimeoutMs)
            {
                try
                {
                    var eval = await cdp.SendAsync("Runtime.evaluate", new JObject
                    {
                        ["expression"] = @"(function(){var el=document.querySelector('h2.title.svelte-zqkpxo'); if(!el) return {exists:false,text:''}; var t=(el.innerText||el.textContent||'').trim(); return {exists:true,text:t};})()",
                        ["returnByValue"] = true
                    });
                    var v = eval? ["result"]? ["result"]? ["value"];
                    if (v != null && (v["exists"]?.ToString() == "True" || v["exists"]?.ToString() == "true"))
                    {
                        var t = v["text"]?.ToString() ?? string.Empty;
                        if (!string.IsNullOrEmpty(t) && t.IndexOf("Чаты", StringComparison.OrdinalIgnoreCase) >= 0)
                            return true;
                    }
                }
                catch {}
                await Task.Delay(pollMs);
            }
            return false;
        }

        private static async Task AutomateFindByNumberAsync(string userDataDir, string phoneNumber, CancellationToken cancellationToken = default)
        {
            try
            {
                Console.WriteLine("[MAX] Начинаю автоматизацию поиска по номеру...");
                
                // Ждем 10 секунд после успешной авторизации
                await Task.Delay(10000, cancellationToken);
                Console.WriteLine("[MAX] Ждал 10 секунд, создаю новое подключение...");
                
                // Создаем новое подключение к браузеру
                await using var cdp = await MaxWebAutomation.ConnectAsync(userDataDir, "web.max.ru");
                Console.WriteLine("[MAX] Новое подключение создано, кликаю через JavaScript...");
                
                // Сразу ищем и кликаем на плюсик через JavaScript
                Console.WriteLine("[MAX] Кликаю на плюсик через JavaScript...");
                await cdp.SendAsync("Runtime.evaluate", new JObject
                {
                    ["expression"] = @"
                        (function() {
                            var buttons = document.querySelectorAll('button');
                            for (var i = 0; i < buttons.length; i++) {
                                var btn = buttons[i];
                                var ariaLabel = btn.getAttribute('aria-label') || '';
                                if (ariaLabel.toLowerCase().indexOf('начать общение') >= 0) {
                                    btn.click();
                                    return true;
                                }
                            }
                            return false;
                        })()
                    ",
                    ["returnByValue"] = true
                });
                
                Console.WriteLine("[MAX] ✅ JavaScript клик выполнен, жду 5 секунд...");
                await Task.Delay(5000); // Ждем открытия меню
                
                // Теперь ищем "Найти по номеру" в появившемся меню через JavaScript
                Console.WriteLine("[MAX] Ищу 'Найти по номеру' в меню...");
                
                // Ищем и кликаем на "Найти по номеру"
                Console.WriteLine("[MAX] Ищу 'Найти по номеру' в меню...");
                var findResult = await cdp.SendAsync("Runtime.evaluate", new JObject
                {
                    ["expression"] = @"
                        (function() {
                            console.log('=== ДИАГНОСТИКА СТРАНИЦЫ ===');
                            
                            // Выводим все видимые элементы с текстом
                            var allElements = Array.from(document.querySelectorAll('*'));
                            var visibleElements = allElements.filter(el => 
                                el.offsetParent !== null && 
                                el.textContent && 
                                el.textContent.trim().length > 0
                            );
                            
                            console.log('Всего видимых элементов с текстом:', visibleElements.length);
                            
                            // Ищем элементы с текстом, содержащим 'найти' или 'номер'
                            var relevantElements = visibleElements.filter(el => 
                                el.textContent.toLowerCase().includes('найти') || 
                                el.textContent.toLowerCase().includes('номер')
                            );
                            
                            console.log('Элементы с найти или номер:', relevantElements.map(el => ({
                                tag: el.tagName,
                                text: el.textContent.trim(),
                                classes: el.className,
                                id: el.id
                            })));
                            
                            // Стратегия 1: Ищем по точному тексту
                            var findElement = visibleElements.find(el => 
                                el.textContent && 
                                el.textContent.trim() === 'Найти по номеру'
                            );
                            
                            if (findElement) {
                                console.log('✅ Найден элемент по точному тексту:', findElement);
                                findElement.click();
                                return { success: true, method: 'exact_text', element: findElement.tagName + ':' + findElement.textContent.trim() };
                            }
                            
                            // Стратегия 2: Ищем по частичному совпадению
                            findElement = visibleElements.find(el => 
                                el.textContent && 
                                el.textContent.includes('Найти по номеру')
                            );
                            
                            if (findElement) {
                                console.log('✅ Найден элемент по частичному совпадению:', findElement);
                                findElement.click();
                                return { success: true, method: 'partial_text', element: findElement.tagName + ':' + findElement.textContent.trim() };
                            }
                            
                            // Стратегия 3: Ищем среди интерактивных элементов
                            var interactiveElements = document.querySelectorAll('button, a, div[role=""button""], div[onclick], div[tabindex]');
                            for (var i = 0; i < interactiveElements.length; i++) {
                                var el = interactiveElements[i];
                                if (el.textContent && el.textContent.includes('Найти по номеру') && el.offsetParent !== null) {
                                    console.log('✅ Найден интерактивный элемент:', el);
                                    el.click();
                                    return { success: true, method: 'interactive', element: el.tagName + ':' + el.textContent.trim() };
                                }
                            }
                            
                            // Стратегия 4: Ищем по классам или атрибутам
                            var classElements = document.querySelectorAll('[class*=""find""], [class*=""search""], [class*=""number""], [data-testid*=""find""]');
                            for (var i = 0; i < classElements.length; i++) {
                                var el = classElements[i];
                                if (el.textContent && el.textContent.includes('номер') && el.offsetParent !== null) {
                                    console.log('✅ Найден элемент по классам:', el);
                                    el.click();
                                    return { success: true, method: 'classes', element: el.tagName + ':' + el.textContent.trim() };
                                }
                            }
                            
                            console.log('Элемент Найти по номеру не найден');
                            return { 
                                success: false, 
                                error: 'Элемент не найден',
                                debug: {
                                    totalVisible: visibleElements.length,
                                    relevant: relevantElements.length,
                                    interactive: interactiveElements.length,
                                    classElements: classElements.length
                                }
                            };
                        })()
                    ",
                    ["returnByValue"] = true
                });
                
                bool clicked = false;
                try
                {
                    var fr1 = findResult["result"] as JObject;
                    var fr2 = fr1 != null ? fr1["result"] as JObject : null;
                    var fval = fr2 != null ? fr2["value"] : null;
                    if (fval != null && fval.Type == JTokenType.Object)
                    {
                        var success = fval["success"];
                        if (success != null && success.Type == JTokenType.Boolean && success.Value<bool>())
                        {
                            clicked = true;
                            var method = fval["method"]?.Value<string>();
                            var element = fval["element"]?.Value<string>();
                            Console.WriteLine($"[MAX] ✅ JavaScript клик 'Найти по номеру' выполнен (метод: {method}, элемент: {element})");
                        }
                        else
                        {
                            var error = fval["error"]?.Value<string>();
                            var debug = fval["debug"] as JObject;
                            Console.WriteLine($"[MAX] ❌ Не удалось кликнуть 'Найти по номеру': {error}");
                            
                            if (debug != null)
                            {
                                Console.WriteLine($"[MAX] 🔍 Отладочная информация:");
                                Console.WriteLine($"[MAX]   - Всего видимых элементов: {debug["totalVisible"]}");
                                Console.WriteLine($"[MAX]   - Релевантных элементов: {debug["relevant"]}");
                                Console.WriteLine($"[MAX]   - Интерактивных элементов: {debug["interactive"]}");
                                Console.WriteLine($"[MAX]   - Элементов по классам: {debug["classElements"]}");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[MAX] ❌ Ошибка при обработке результата клика: {ex.Message}");
                }
                
                if (!clicked)
                {
                    Console.WriteLine("[MAX] ⚠️ Не удалось нажать 'Найти по номеру'");
                }
                else
                {
                    // Ждем 5 секунд после нажатия "Найти по номеру" для загрузки поля ввода
                    Console.WriteLine("[MAX] Жду 5 секунд после нажатия 'Найти по номеру'...");
                    await Task.Delay(5000);
                    
                    // Дополнительная проверка - ждем загрузки модального окна
                    Console.WriteLine("[MAX] Дополнительно жду 3 секунды для загрузки модального окна...");
                    await Task.Delay(3000);
                    
                    // Находим пользователя по userDataDir (нужно передать userId)
                    long? userId = null;
                    foreach (var kvp in _lastSessionDirByUser)
                    {
                        if (kvp.Value == userDataDir)
                        {
                            userId = kvp.Key;
                            break;
                        }
                    }
                    
                    if (userId.HasValue)
                    {
                        // Получаем последний использованный номер для этого пользователя
                        var excludeNumbers = new List<string>();
                        if (_lastUsedNumberByUser.TryGetValue(userId.Value, out var lastUsedNumber))
                        {
                            excludeNumbers.Add(lastUsedNumber);
                        }
                        
                        // Исключаем также текущий номер, который авторизуется
                        var currentPhoneNormalized = new string(phoneNumber.Where(char.IsDigit).ToArray());
                        if (currentPhoneNormalized.StartsWith("7")) currentPhoneNormalized = currentPhoneNormalized.Substring(1);
                        if (currentPhoneNormalized.StartsWith("8")) currentPhoneNormalized = currentPhoneNormalized.Substring(1);
                        if (currentPhoneNormalized.Length > 10) currentPhoneNormalized = currentPhoneNormalized.Substring(currentPhoneNormalized.Length - 10);
                        
                        if (!excludeNumbers.Contains(currentPhoneNormalized))
                        {
                            excludeNumbers.Add(currentPhoneNormalized);
                        }
                        
                        Console.WriteLine($"[MAX] Исключаем номера: {string.Join(", ", excludeNumbers)}");
                        
                        // Бесконечный цикл ожидания нового номера для прогрева
                        Console.WriteLine("[MAX] 🔄 Начинаю бесконечный цикл прогрева номеров...");
                        
                        // Список всех прогревающихся номеров для равномерного выбора
                        var allWarmingNumbers = new List<string>();
                        var currentCycleLastNumber = ""; // Для отслеживания последнего использованного номера в текущем цикле
                        var consecutiveUses = 0; // Счетчик подряд использований одного номера
                        
                        // ВАЖНО: Проверяем, что свой номер не в списке прогревающихся
                        Console.WriteLine($"[MAX] 🚫 ТЕКУЩИЙ НОМЕР: {currentPhoneNormalized} - он НЕ может прогреваться сам с собой!");
                        
                        // ВАЖНО: Очищаем список прогревающихся номеров от своего номера
                        var initialWarmingNumbers = _warmingCtsByPhone.Keys.ToList();
                        var cleanWarmingNumbers = initialWarmingNumbers.Where(phone => phone != currentPhoneNormalized).ToList();
                        Console.WriteLine($"[MAX] 🚫 Инициализация: исключил свой номер {currentPhoneNormalized} из {initialWarmingNumbers.Count} прогревающихся. Осталось: {cleanWarmingNumbers.Count}");
                        
                        // ВАЖНО: Инициализируем allWarmingNumbers только чистыми номерами
                        allWarmingNumbers.Clear();
                        foreach (var phone in cleanWarmingNumbers)
                        {
                            // ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: никогда не добавляем свой номер
                            if (phone != currentPhoneNormalized)
                            {
                                allWarmingNumbers.Add(phone);
                                Console.WriteLine($"[MAX] 🔥 Инициализация: добавил прогревающийся номер в общий список: {phone}");
                            }
                            else
                            {
                                Console.WriteLine($"[MAX] 🚫 Инициализация: ПРОПУСКАЮ свой номер {phone} - он не может прогреваться сам с собой!");
                            }
                        }
                        
                        while (true)
                        {
                            // Проверяем отмену операции
                            if (cancellationToken.IsCancellationRequested)
                            {
                                Console.WriteLine("[MAX] ❌ Операция отменена пользователем, прекращаю работу");
                                break;
                            }
                            
                            // Проверяем, не завершился ли прогрев для текущего номера
                            if (!_warmingCtsByPhone.ContainsKey(phoneNumber))
                            {
                                Console.WriteLine($"[MAX] ❌ Прогрев для номера {phoneNumber} завершен, прекращаю работу");
                                break;
                            }
                            
                            // Проверяем, не закрылся ли браузер (прогрев мог завершиться)
                            try
                            {
                                var browserCheck = await cdp.SendAsync("Runtime.evaluate", new JObject
                                {
                                    ["expression"] = "document.readyState",
                                    ["returnByValue"] = true
                                });
                                
                                if (browserCheck == null || browserCheck["result"] == null)
                                {
                                    Console.WriteLine("[MAX] ❌ Браузер закрыт (прогрев завершился), прекращаю работу");
                                    break;
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[MAX] ❌ Браузер недоступен (прогрев завершился): {ex.Message}");
                                break;
                            }
                            
                            // Обновляем список прогревающихся номеров
                            var warmingNumbers = _warmingCtsByPhone.Keys.ToList();
                            
                            // ВАЖНО: Сразу исключаем свой собственный номер из списка прогревающихся
                            warmingNumbers = warmingNumbers.Where(phone => phone != currentPhoneNormalized).ToList();
                            Console.WriteLine($"[MAX] 🚫 Исключил свой номер {currentPhoneNormalized} из списка прогревающихся. Доступно: {warmingNumbers.Count}");
                            
                            // Если появились новые прогревающиеся номера, добавляем их в общий список
                            // НО НИКОГДА не добавляем свой собственный номер!
                            foreach (var phone in warmingNumbers)
                            {
                                // ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: никогда не добавляем свой номер
                                if (phone == currentPhoneNormalized)
                                {
                                    Console.WriteLine($"[MAX] 🚫 ПРОПУСКАЮ свой номер {phone} - он не может прогреваться сам с собой!");
                                    continue;
                                }
                                
                                if (!allWarmingNumbers.Contains(phone))
                                {
                                    allWarmingNumbers.Add(phone);
                                    Console.WriteLine($"[MAX] 🔥 Добавил новый прогревающийся номер в общий список: {phone}");
                                }
                            }
                            
                            // Убираем номера, которые больше не прогреваются
                            allWarmingNumbers.RemoveAll(phone => !warmingNumbers.Contains(phone));
                            
                            // ВАЖНО: Дополнительная проверка - убираем свой номер из allWarmingNumbers если он там есть
                            if (allWarmingNumbers.Contains(currentPhoneNormalized))
                            {
                                Console.WriteLine($"[MAX] 🚫 КРИТИЧЕСКАЯ ОШИБКА: Свой номер {currentPhoneNormalized} попал в allWarmingNumbers! Убираю...");
                                allWarmingNumbers.Remove(currentPhoneNormalized);
                            }
                            
                            // ВАЖНО: Дополнительная проверка - убираем свой номер из allWarmingNumbers если он там есть (по нормализованному номеру)
                            var normalizedWarmingNumbers = allWarmingNumbers.Where(phone => 
                            {
                                var normalizedPhone = new string(phone.Where(char.IsDigit).ToArray());
                                if (normalizedPhone.StartsWith("7")) normalizedPhone = normalizedPhone.Substring(1);
                                if (normalizedPhone.StartsWith("8")) normalizedPhone = normalizedPhone.Substring(1);
                                if (normalizedPhone.Length > 10) normalizedPhone = normalizedPhone.Substring(normalizedPhone.Length - 10);
                                return normalizedPhone != currentPhoneNormalized;
                            }).ToList();
                            
                            if (normalizedWarmingNumbers.Count != allWarmingNumbers.Count)
                            {
                                Console.WriteLine($"[MAX] 🚫 КРИТИЧЕСКАЯ ОШИБКА: Свой номер {currentPhoneNormalized} попал в allWarmingNumbers по нормализованному номеру! Убираю...");
                                allWarmingNumbers.Clear();
                                allWarmingNumbers.AddRange(normalizedWarmingNumbers);
                            }
                            
                            // Фильтруем доступные номера (исключаем уже использованные в этом цикле и свой собственный номер)
                            var availableWarmingNumbers = allWarmingNumbers.Where(phone => 
                                !excludeNumbers.Contains(phone) && 
                                phone != currentPhoneNormalized // Никогда не выбираем свой собственный номер
                            ).ToList();
                            
                            string? numberToUse = null;
                            
                            // Если есть доступные прогревающиеся номера, выбираем с учетом равномерности
                            if (availableWarmingNumbers.Count > 0)
                            {
                                // Проверяем, не застряли ли мы на одном номере
                                if (availableWarmingNumbers.Count == 1 && 
                                    availableWarmingNumbers[0] == currentCycleLastNumber && 
                                    consecutiveUses >= 2)
                                {
                                    // Застряли на одном номере, принудительно очищаем список исключений
                                    Console.WriteLine($"[MAX] ⚠️ Застрял на номере {currentCycleLastNumber} ({consecutiveUses} раз подряд), принудительно начинаю новый цикл...");
                                    excludeNumbers.Clear();
                                    consecutiveUses = 0;
                                    
                                    // Снова выбираем случайный прогревающийся номер
                                    var randomWarmingIndex = new Random().Next(0, allWarmingNumbers.Count);
                                    numberToUse = allWarmingNumbers[randomWarmingIndex];
                                    Console.WriteLine($"[MAX] 🔥 Принудительно начинаю новый цикл с номером: {numberToUse}");
                                }
                                else
                                {
                                    // Обычный выбор случайного номера
                                    var randomWarmingIndex = new Random().Next(0, availableWarmingNumbers.Count);
                                    numberToUse = availableWarmingNumbers[randomWarmingIndex];
                                    
                                    // Проверяем, не тот ли это номер, что использовался последним
                                    if (numberToUse == currentCycleLastNumber)
                                    {
                                        consecutiveUses++;
                                        Console.WriteLine($"[MAX] 🔥 Выбираю прогревающийся номер: {numberToUse} (доступно: {availableWarmingNumbers.Count}, использован {consecutiveUses} раз подряд)");
                                    }
                                    else
                                    {
                                        consecutiveUses = 1; // Сбрасываем счетчик
                                        Console.WriteLine($"[MAX] 🔥 Выбираю прогревающийся номер: {numberToUse} (доступно: {availableWarmingNumbers.Count})");
                                    }
                                }
                            }
                            else if (allWarmingNumbers.Count > 0)
                            {
                                // Если все прогревающиеся номера уже использованы, очищаем список исключений
                                // и начинаем новый цикл прогрева всех номеров
                                Console.WriteLine($"[MAX] 🔄 Все {allWarmingNumbers.Count} прогревающихся номеров уже использованы, начинаю новый цикл...");
                                excludeNumbers.Clear();
                                consecutiveUses = 0; // Сбрасываем счетчик
                                
                                // Снова выбираем случайный прогревающийся номер
                                var randomWarmingIndex = new Random().Next(0, allWarmingNumbers.Count);
                                numberToUse = allWarmingNumbers[randomWarmingIndex];
                                Console.WriteLine($"[MAX] 🔥 Начинаю новый цикл с номером: {numberToUse}");
                            }
                            else
                            {
                                // Если нет прогревающихся номеров (потому что свой номер исключен), сразу ищем новый
                                Console.WriteLine("[MAX] 🔍 Нет прогревающихся номеров (свой номер исключен), ищу новый...");
                                
                                // Добавляем свой номер в исключения для поиска новых номеров
                                var extendedExcludeNumbers = new List<string>(excludeNumbers);
                                if (!extendedExcludeNumbers.Contains(currentPhoneNormalized))
                                {
                                    extendedExcludeNumbers.Add(currentPhoneNormalized);
                                    Console.WriteLine($"[MAX] 🚫 Добавляю свой номер {currentPhoneNormalized} в исключения для поиска новых номеров");
                                }
                                
                                var newRandomNumber = await _supabaseService.GetRandomPhoneNumberAsync(userId.Value, extendedExcludeNumbers);
                                
                                if (!string.IsNullOrEmpty(newRandomNumber))
                                {
                                    numberToUse = newRandomNumber;
                                    Console.WriteLine($"[MAX] 🎯 Найден новый номер для прогрева: {numberToUse}");
                                }
                            }
                            // Убираем дублирующийся блок else
                            
                            if (!string.IsNullOrEmpty(numberToUse))
                            {
                                // Дополнительная проверка - никогда не используем свой собственный номер
                                if (numberToUse == currentPhoneNormalized)
                                {
                                    Console.WriteLine($"[MAX] 🚫 ОШИБКА: Попытка выбрать свой собственный номер {numberToUse} для прогрева! Пропускаю...");
                                    excludeNumbers.Add(numberToUse);
                                    continue; // Переходим к следующей итерации
                                }
                                
                            // Нормализуем номер для ввода (убираем + и оставляем только цифры)
                                var normalizedNumber = new string(numberToUse.Where(char.IsDigit).ToArray());
                            if (normalizedNumber.StartsWith("7")) normalizedNumber = normalizedNumber.Substring(1);
                            if (normalizedNumber.StartsWith("8")) normalizedNumber = normalizedNumber.Substring(1);
                            if (normalizedNumber.Length > 10) normalizedNumber = normalizedNumber.Substring(normalizedNumber.Length - 10);
                            
                                Console.WriteLine($"[MAX] Ввожу номер для прогрева: {normalizedNumber}");
                            
                                            // Вводим номер через JavaScript
                Console.WriteLine("[MAX] Отправляю JavaScript для ввода номера...");
                var inputResult = await cdp.SendAsync("Runtime.evaluate", new JObject
                {
                    ["expression"] = $@"
                        (function() {{
                            console.log('=== ПРОСТОЙ ВВОД НОМЕРА ===');
                            
                            // Ищем ТОЛЬКО внутри модального окна
                            var modal = document.querySelector('dialog[data-testid=""modal""]') || document.querySelector('dialog[open]') || document.querySelector('.modal');
                            if (!modal) {{
                                console.log('МОДАЛЬНОЕ ОКНО НЕ НАЙДЕНО');
                                return {{ success: false, error: 'Модальное окно не найдено' }};
                            }}
                            
                            // Ищем поле ввода ТОЛЬКО внутри модального окна
                            var targetInput = modal.querySelector('input.field.svelte-12kaleq') || 
                                             modal.querySelector('input[placeholder*=""+7 000 000-00-00""]') || 
                                             modal.querySelector('input.field') ||
                                             modal.querySelector('input[type=""text""]');
                            
                            if (targetInput) {{
                                console.log('НАЙДЕНО ПОЛЕ:', targetInput);
                                targetInput.value = '{normalizedNumber}';
                                targetInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
                                console.log('НОМЕР ВВЕДЕН:', targetInput.value);
                                
                                // Номер введен, возвращаем успех
                                console.log('НОМЕР УСПЕШНО ВВЕДЕН, КНОПКА БУДЕТ НАЖАТА ПОЗЖЕ');
                                return {{ success: true, buttonClicked: false }};
                            }} else {{
                                console.log('ПОЛЕ НЕ НАЙДЕНО');
                                return {{ success: false, error: 'Поле не найдено' }};
                            }}
                        }})()
                    ",
                    ["returnByValue"] = true
                });
                Console.WriteLine("[MAX] JavaScript для ввода номера отправлен");
                            
                            // Проверяем результат ввода
                            try
                            {
                                bool inputSuccess = false;
                                var ir1 = inputResult["result"] as JObject;
                                var ir2 = ir1 != null ? ir1["result"] as JObject : null;
                                var ival = ir2 != null ? ir2["value"] : null;
                                
                                if (ival != null && ival.Type == JTokenType.Object)
                                {
                                    var successToken = ival["success"];
                                    if (successToken != null && successToken.Type == JTokenType.Boolean)
                                        inputSuccess = successToken.Value<bool>();
                                    
                                    if (inputSuccess)
                                    {
                                        var buttonClicked = ival["buttonClicked"]?.Value<bool>() ?? false;
                                        
                                        // Сохраняем номер как последний использованный
                                            _lastUsedNumberByUser[userId.Value] = numberToUse;
                                        
                                        if (buttonClicked)
                                        {
                                            Console.WriteLine($"[MAX] ✅ Случайный номер {normalizedNumber} успешно введен и кнопка нажата");
                                        }
                                        else
                                        {
                                            Console.WriteLine($"[MAX] ✅ Случайный номер {normalizedNumber} успешно введен, но кнопка не найдена");
                                        }
                                        
                                        // Ждем 5 секунд после ввода номера перед нажатием кнопки
                                        Console.WriteLine("[MAX] Жду 5 секунд после ввода номера...");
                                await Task.Delay(5000, cancellationToken);
                                        
                                        // Теперь ищем и нажимаем кнопку
                                        Console.WriteLine("[MAX] Ищу кнопку 'Найти в Max' для нажатия...");
                                        var buttonResult = await cdp.SendAsync("Runtime.evaluate", new JObject
                                        {
                                            ["expression"] = @"
                                                (function() {
                                                    var modal = document.querySelector('dialog[data-testid=""modal""]') || document.querySelector('dialog[open]') || document.querySelector('.modal');
                                                    if (!modal) {
                                                        console.log('МОДАЛЬНОЕ ОКНО НЕ НАЙДЕНО');
                                                        return { success: false, error: 'Модальное окно не найдено' };
                                                    }
                                                    
                                                    var submitButton = modal.querySelector('button[form=""findContact""]') || modal.querySelector('button[aria-label=""Найти в Max""]');
                                                    if (submitButton) {
                                                        console.log('НАЙДЕНА КНОПКА ДЛЯ НАЖАТИЯ:', submitButton);
                                                        submitButton.click();
                                                        console.log('КНОПКА НАЖАТА');
                                                        return { success: true, buttonClicked: true };
                                                    } else {
                                                        console.log('КНОПКА НЕ НАЙДЕНА');
                                                        return { success: false, error: 'Кнопка не найдена' };
                                                    }
                                                })()
                                            ",
                                            ["returnByValue"] = true
                                        });
                                        
                                        // Проверяем результат нажатия кнопки
                                        bool buttonSuccess = false;
                                        try
                                        {
                                            var br1 = buttonResult["result"] as JObject;
                                            var br2 = br1 != null ? br1["result"] as JObject : null;
                                            var bval = br2 != null ? br2["value"] : null;
                                            
                                            if (bval != null && bval.Type == JTokenType.Object)
                                            {
                                                buttonSuccess = bval["success"]?.Value<bool>() ?? false;
                                                if (buttonSuccess)
                                                {
                                                    Console.WriteLine("[MAX] ✅ Кнопка 'Найти в Max' успешно нажата");
                                                }
                                                else
                                                {
                                                    var error = bval["error"]?.Value<string>();
                                                    Console.WriteLine($"[MAX] ❌ Ошибка нажатия кнопки: {error}");
                                                }
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            Console.WriteLine($"[MAX] ❌ Ошибка при обработке результата нажатия кнопки: {ex.Message}");
                                        }
                                        
                                            // Если кнопка нажата успешно, ждем 10 секунд и вводим сообщения
                                        if (buttonSuccess)
                                        {
                                                Console.WriteLine("[MAX] Жду 10 секунд перед началом отправки сообщений...");
                                            await Task.Delay(10000);
                                                
                                                // Отправляем от 10 до 15 сообщений
                                                var messageCount = new Random().Next(10, 16); // 10-15 сообщений
                                                Console.WriteLine($"[MAX] 🚀 Начинаю отправку {messageCount} сообщений для прогрева...");
                                                
                                                for (int msgIndex = 1; msgIndex <= messageCount; msgIndex++)
                                                {
                                                    try
                                                    {
                                                        Console.WriteLine($"[MAX] 📝 Отправляю сообщение {msgIndex}/{messageCount}...");
                                            
                                            // Вводим случайное сообщение из шаблона
                                            await SendRandomMessageAsync(cdp);
                                                        
                                                        var (minDelay, maxDelay) = GetMessageInterval(phoneNumber);
                                                        var delaySeconds = new Random().Next(minDelay, maxDelay + 1);
                                                        Console.WriteLine($"[MAX] ⏳ Жду {delaySeconds} секунд перед следующим сообщением...");
                                                        await Task.Delay(delaySeconds * 1000);
                                                        
                                                        Console.WriteLine($"[MAX] ✅ Сообщение {msgIndex}/{messageCount} отправлено!");
                                                    }
                                                    catch (Exception ex)
                                                    {
                                                        Console.WriteLine($"[MAX] ❌ Ошибка при отправке сообщения {msgIndex}: {ex.Message}");
                                                        // Продолжаем отправку других сообщений
                                                        await Task.Delay(5000);
                                                    }
                                                }
                                                
                                                Console.WriteLine($"[MAX] 🎉 Все {messageCount} сообщений отправлены! Жду 30 секунд перед переходом к новому номеру...");
                                                await Task.Delay(30000);
                                                
                                                // Обновляем список исключаемых номеров
                                                excludeNumbers.Add(numberToUse);
                                                
                                                // Обновляем информацию о последнем использованном номере
                                                currentCycleLastNumber = numberToUse;
                                                
                                                // Закрываем текущий поиск и возвращаемся к главному экрану
                                                Console.WriteLine("[MAX] 🔄 Закрываю текущий поиск и возвращаюсь к главному экрану...");
                                                try
                                                {
                                                    // Пытаемся закрыть модальное окно поиска
                                                    await cdp.SendAsync("Runtime.evaluate", new JObject
                                                    {
                                                        ["expression"] = @"
                                                            (function() {
                                                                // Ищем кнопку закрытия или крестик
                                                                var closeButton = document.querySelector('button[aria-label*=""Закрыть""]') ||
                                                                                 document.querySelector('button[aria-label*=""Close""]') ||
                                                                                 document.querySelector('button.close') ||
                                                                                 document.querySelector('button[class*=""close""]') ||
                                                                                 document.querySelector('svg[href=""#icon_close_24""]')?.closest('button') ||
                                                                                 document.querySelector('[data-testid=""close""]');
                                                                
                                                                if (closeButton) {
                                                                    closeButton.click();
                                                                    return { success: true, action: 'close_button_clicked' };
                                                                }
                                                                
                                                                // Если кнопка закрытия не найдена, пытаемся нажать Escape
                                                                document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', keyCode: 27 }));
                                                                return { success: true, action: 'escape_pressed' };
                                                            })()
                                                        ",
                                                        ["returnByValue"] = true
                                                    });
                                                    
                                                    // Ждем закрытия модального окна
                                                    await Task.Delay(2000);
                                                    
                                                    // Снова нажимаем на плюсик для нового поиска
                                                    Console.WriteLine("[MAX] 🔄 Нажимаю на плюсик для нового поиска...");
                                                    await cdp.SendAsync("Runtime.evaluate", new JObject
                                                    {
                                                        ["expression"] = @"
                                                            (function() {
                                                                var buttons = document.querySelectorAll('button');
                                                                for (var i = 0; i < buttons.length; i++) {
                                                                    var btn = buttons[i];
                                                                    var ariaLabel = btn.getAttribute('aria-label') || '';
                                                                    if (ariaLabel.toLowerCase().indexOf('начать общение') >= 0) {
                                                                        btn.click();
                                                                        return { success: true, action: 'plus_clicked' };
                                                                    }
                                                                }
                                                                return { success: false, error: 'Плюсик не найден' };
                                                            })()
                                                        ",
                                                        ["returnByValue"] = true
                                                    });
                                                    
                                                    // Ждем открытия меню
                                                    await Task.Delay(3000);
                                                    
                                                    // Снова нажимаем "Найти по номеру" - используем точно такой же код как в начале
                                                    Console.WriteLine("[MAX] 🔄 Нажимаю 'Найти по номеру' для нового поиска...");
                                                    var findResult2 = await cdp.SendAsync("Runtime.evaluate", new JObject
                                                    {
                                                        ["expression"] = @"
                                                            (function() {
                                                                console.log('=== ДИАГНОСТИКА СТРАНИЦЫ (ПОВТОРНЫЙ ПОИСК) ===');
                                                                
                                                                // Выводим все видимые элементы с текстом
                                                                var allElements = Array.from(document.querySelectorAll('*'));
                                                                var visibleElements = allElements.filter(el => 
                                                                    el.offsetParent !== null && 
                                                                    el.textContent && 
                                                                    el.textContent.trim().length > 0
                                                                );
                                                                
                                                                console.log('Всего видимых элементов с текстом:', visibleElements.length);
                                                                
                                                                // Ищем элементы с текстом, содержащим 'найти' или 'номер'
                                                                var relevantElements = visibleElements.filter(el => 
                                                                    el.textContent.toLowerCase().includes('найти') || 
                                                                    el.textContent.toLowerCase().includes('номер')
                                                                );
                                                                
                                                                console.log('Элементы с найти или номер:', relevantElements.map(el => ({
                                                                    tag: el.tagName,
                                                                    text: el.textContent.trim(),
                                                                    classes: el.className,
                                                                    id: el.id
                                                                })));
                                                                
                                                                // Стратегия 1: Ищем по точному тексту
                                                                var findElement = visibleElements.find(el => 
                                                                    el.textContent && 
                                                                    el.textContent.trim() === 'Найти по номеру'
                                                                );
                                                                
                                                                if (findElement) {
                                                                    console.log('✅ Найден элемент по точному тексту:', findElement);
                                                                    findElement.click();
                                                                    return { success: true, method: 'exact_text', element: findElement.tagName + ':' + findElement.textContent.trim() };
                                                                }
                                                                
                                                                // Стратегия 2: Ищем по частичному совпадению
                                                                findElement = visibleElements.find(el => 
                                                                    el.textContent && 
                                                                    el.textContent.includes('Найти по номеру')
                                                                );
                                                                
                                                                if (findElement) {
                                                                    console.log('✅ Найден элемент по частичному совпадению:', findElement);
                                                                    findElement.click();
                                                                    return { success: true, method: 'partial_text', element: findElement.tagName + ':' + findElement.textContent.trim() };
                                                                }
                                                                
                                                                // Стратегия 3: Ищем среди интерактивных элементов
                                                                var interactiveElements = document.querySelectorAll('button, a, div[role=""button""], div[onclick], div[tabindex]');
                                                                for (var i = 0; i < interactiveElements.length; i++) {
                                                                    var el = interactiveElements[i];
                                                                    if (el.textContent && el.textContent.includes('Найти по номеру') && el.offsetParent !== null) {
                                                                        console.log('✅ Найден интерактивный элемент:', el);
                                                                        el.click();
                                                                        return { success: true, method: 'interactive', element: el.tagName + ':' + el.textContent.trim() };
                                                                    }
                                                                }
                                                                
                                                                // Стратегия 4: Ищем по классам или атрибутам
                                                                var classElements = document.querySelectorAll('[class*=""find""], [class*=""search""], [class*=""number""], [data-testid*=""find""]');
                                                                for (var i = 0; i < classElements.length; i++) {
                                                                    var el = classElements[i];
                                                                    if (el.textContent && el.textContent.includes('номер') && el.offsetParent !== null) {
                                                                        console.log('✅ Найден элемент по классам:', el);
                                                                        el.click();
                                                                        return { success: true, method: 'classes', element: el.tagName + ':' + el.textContent.trim() };
                                                                    }
                                                                }
                                                                
                                                                console.log('Элемент Найти по номеру не найден');
                                                                return { 
                                                                    success: false, 
                                                                    error: 'Элемент не найден',
                                                                    debug: {
                                                                        totalVisible: visibleElements.length,
                                                                        relevant: relevantElements.length,
                                                                        interactive: interactiveElements.length,
                                                                        classElements: classElements.length
                                                                    }
                                                                };
                                                            })()
                                                        ",
                                                        ["returnByValue"] = true
                                                    });
                                                    
                                                    // Проверяем результат нажатия "Найти по номеру"
                                                    bool findClicked = false;
                                                    try
                                                    {
                                                        var fr1_2 = findResult2["result"] as JObject;
                                                        var fr2_2 = fr1_2 != null ? fr1_2["result"] as JObject : null;
                                                        var fval_2 = fr2_2 != null ? fr2_2["value"] : null;
                                                        if (fval_2 != null && fval_2.Type == JTokenType.Object)
                                                        {
                                                            var success = fval_2["success"];
                                                            if (success != null && success.Type == JTokenType.Boolean && success.Value<bool>())
                                                            {
                                                                findClicked = true;
                                                                var method = fval_2["method"]?.Value<string>();
                                                                var element = fval_2["element"]?.Value<string>();
                                                                Console.WriteLine($"[MAX] ✅ JavaScript клик 'Найти по номеру' (повторный) выполнен (метод: {method}, элемент: {element})");
                                                            }
                                                            else
                                                            {
                                                                var error = fval_2["error"]?.Value<string>();
                                                                var debug = fval_2["debug"] as JObject;
                                                                Console.WriteLine($"[MAX] ❌ Не удалось кликнуть 'Найти по номеру' (повторный): {error}");
                                                                
                                                                if (debug != null)
                                                                {
                                                                    Console.WriteLine($"[MAX] 🔍 Отладочная информация (повторный поиск):");
                                                                    Console.WriteLine($"[MAX]   - Всего видимых элементов: {debug["totalVisible"]}");
                                                                    Console.WriteLine($"[MAX]   - Релевантных элементов: {debug["relevant"]}");
                                                                    Console.WriteLine($"[MAX]   - Интерактивных элементов: {debug["interactive"]}");
                                                                    Console.WriteLine($"[MAX]   - Элементов по классам: {debug["classElements"]}");
                                                                }
                                                            }
                                                        }
                                                    }
                                                    catch (Exception ex)
                                                    {
                                                        Console.WriteLine($"[MAX] ❌ Ошибка при обработке результата клика (повторный): {ex.Message}");
                                                    }
                                                    
                                                    if (findClicked)
                                                    {
                                                        // Ждем 5 секунд после нажатия "Найти по номеру" для загрузки поля ввода
                                                        Console.WriteLine("[MAX] Жду 5 секунд после нажатия 'Найти по номеру' (повторный)...");
                                                        await Task.Delay(5000);
                                                        
                                                        // Дополнительная проверка - ждем загрузки модального окна
                                                        Console.WriteLine("[MAX] Дополнительно жду 3 секунды для загрузки модального окна (повторный)...");
                                                        await Task.Delay(3000);
                                                        
                                                        Console.WriteLine("[MAX] ✅ Готов к поиску следующего номера!");
                                                    }
                                                    else
                                                    {
                                                        Console.WriteLine("[MAX] ⚠️ Не удалось нажать 'Найти по номеру' (повторный), но продолжаю...");
                                                        // Ждем немного и продолжаем
                                                        await Task.Delay(3000);
                                                    }
                                                }
                                                catch (Exception ex)
                                                {
                                                    Console.WriteLine($"[MAX] ⚠️ Ошибка при подготовке к новому поиску: {ex.Message}");
                                                }
                                                
                                                // Продолжаем цикл для поиска следующего номера
                                                Console.WriteLine("[MAX] 🔄 Продолжаю поиск следующего номера для прогрева...");
                                                continue;
                                            }
                                            else
                                            {
                                                // Если кнопка не нажата, все равно продолжаем цикл
                                                Console.WriteLine("[MAX] Кнопка не нажата, но продолжаю поиск следующего номера...");
                                                excludeNumbers.Add(numberToUse);
                                                continue;
                                        }
                                    }
                                    else
                                    {
                                        var error = ival["error"]?.Value<string>();
                                        Console.WriteLine($"[MAX] ❌ Не удалось найти поле ввода для номера: {error}");
                                            // Даже при ошибке продолжаем цикл
                                            excludeNumbers.Add(numberToUse);
                                            continue;
                                    }
                                }
                                else if (ival != null && ival.Type == JTokenType.Boolean && ival.Value<bool>())
                                {
                                    // Обратная совместимость со старым форматом
                                        _lastUsedNumberByUser[userId.Value] = numberToUse;
                                        Console.WriteLine($"[MAX] ✅ Номер {normalizedNumber} успешно введен");
                                        // Продолжаем цикл
                                        excludeNumbers.Add(numberToUse);
                                        continue;
                                }
                                else
                                {
                                    Console.WriteLine("[MAX] ❌ Не удалось найти поле ввода для номера");
                                        // Даже при ошибке продолжаем цикл
                                        excludeNumbers.Add(numberToUse);
                                        continue;
                                }
                            }
                            catch 
                            {
                                Console.WriteLine("[MAX] ❌ Ошибка при проверке результата ввода номера");
                                    // Даже при ошибке продолжаем цикл
                                    excludeNumbers.Add(numberToUse);
                                    continue;
                            }
                        }
                        else
                        {
                                Console.WriteLine("[MAX] ⏳ Нет доступных номеров для прогрева, жду 60 секунд...");
                                
                                // Дополнительная проверка: не завершился ли прогрев
                                if (!_warmingCtsByPhone.ContainsKey(phoneNumber))
                                {
                                    Console.WriteLine($"[MAX] ❌ Прогрев для номера {phoneNumber} завершен во время ожидания, прекращаю работу");
                                    break;
                                }
                                
                                // Проверяем отмену операции
                                if (cancellationToken.IsCancellationRequested)
                                {
                                    Console.WriteLine("[MAX] ❌ Операция отменена пользователем во время ожидания, прекращаю работу");
                                    break;
                                }
                                
                                await Task.Delay(60000, cancellationToken); // Ждем 1 минуту перед следующей попыткой
                                
                                // Проверяем, не закрылся ли браузер
                                try
                                {
                                    var testResult = await cdp.SendAsync("Runtime.evaluate", new JObject
                                    {
                                        ["expression"] = "document.readyState",
                                        ["returnByValue"] = true
                                    });
                                    
                                    if (testResult == null || testResult["result"] == null)
                                    {
                                        Console.WriteLine("[MAX] ❌ Браузер закрыт, прекращаю ожидание");
                                        break;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"[MAX] ❌ Ошибка проверки состояния браузера: {ex.Message}");
                                    Console.WriteLine("[MAX] ❌ Браузер закрыт, прекращаю ожидание");
                                    break;
                                }
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine("[MAX] ⚠️ Не удалось определить пользователя");
                    }
                }
                
                Console.WriteLine("[MAX] ✅ JavaScript поиск 'Найти по номеру' выполнен!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MAX] Ошибка автоматизации поиска по номеру: {ex.Message}");
            }
        }

        private static void StartWarmingTimer(string phoneNumber, long chatId, TimeSpan? customDuration = null)
        {
            try
            {
                // Сначала проверяем, есть ли сохраненный остаток времени
                var hasRemaining = _warmingRemainingByPhone.TryGetValue(phoneNumber, out var remain);
                var duration = customDuration ?? (hasRemaining && remain > TimeSpan.Zero
                    ? remain
                    : TimeSpan.FromHours(6));

                // Если уже идет прогрев — перезапускаем (браузер не закрываем)
                StopWarmingTimer(phoneNumber, saveRemaining: false, closeBrowser: false); // Не сохраняем, так как уже знаем duration

                // Очищаем сохраненный остаток, так как он теперь используется
                if (hasRemaining)
                {
                    _warmingRemainingByPhone.Remove(phoneNumber);
                    SaveWarmingState();
                }

                var endsAt = DateTime.UtcNow.Add(duration);
                _warmingEndsByPhone[phoneNumber] = endsAt;
                var cts = new CancellationTokenSource();
                _warmingCtsByPhone[phoneNumber] = cts;
                SaveWarmingState();

                _ = Task.Run(async () =>
                {
                    bool finishedNaturally = false;
                    try
                    {
                        await _botClient.SendTextMessageAsync(chatId, $"🔥 Запущен прогрев для {phoneNumber}\n⏳ Осталось: {duration:hh\\:mm\\:ss}");

                        while (!cts.IsCancellationRequested)
                        {
                            var now = DateTime.UtcNow;
                            var left = endsAt - now;
                            if (left <= TimeSpan.Zero) { finishedNaturally = true; break; }
                            _warmingRemainingByPhone[phoneNumber] = left;
                            SaveWarmingState();
                            await Task.Delay(TimeSpan.FromMinutes(1), cts.Token);
                        }
                    }
                    catch { }
                    finally
                    {
                        _warmingCtsByPhone.Remove(phoneNumber);
                        _warmingEndsByPhone.Remove(phoneNumber);
                        if (finishedNaturally)
                        {
                            _warmingRemainingByPhone.Remove(phoneNumber);
                            SaveWarmingState();
                            
                            // Закрываем браузер для этого номера
                            try 
                            { 
                                Console.WriteLine($"[WARMING] 🔄 Закрываю браузер для завершенного прогрева номера {phoneNumber}");
                                
                                // Получаем директорию сессии для этого номера
                                if (_sessionDirByPhone.TryGetValue(phoneNumber, out var sessionDir) && !string.IsNullOrEmpty(sessionDir))
                                {
                                    Console.WriteLine($"[WARMING] 📁 Найдена директория сессии: {sessionDir}");
                                    
                                    // Пытаемся подключиться к браузеру и закрыть его
                                    try
                                    {
                                        await using var cdp = await MaxWebAutomation.ConnectAsync(sessionDir, "web.max.ru");
                                        await cdp.CloseBrowserAsync();
                                        Console.WriteLine($"[WARMING] ✅ Браузер для номера {phoneNumber} успешно закрыт");
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"[WARMING] ⚠️ Ошибка при закрытии браузера для номера {phoneNumber}: {ex.Message}");
                                    }
                                    
                                    // Удаляем директорию сессии
                                    _sessionDirByPhone.Remove(phoneNumber);
                                    Console.WriteLine($"[WARMING] 🗑️ Директория сессии для номера {phoneNumber} удалена");
                                }
                                else
                                {
                                    Console.WriteLine($"[WARMING] ⚠️ Директория сессии для номера {phoneNumber} не найдена");
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[WARMING] ❌ Ошибка при закрытии браузера для номера {phoneNumber}: {ex.Message}");
                            }
                            
                            try { await _botClient.SendTextMessageAsync(chatId, $"✅ Прогрев для {phoneNumber} завершен."); } catch { }
                            try
                            {
                                var norm = SupabaseService.NormalizePhoneForActive(phoneNumber);
                                if (!string.IsNullOrEmpty(norm))
                                    await _supabaseService.DeleteActiveNumberByPhoneAsync(norm);
                            }
                            catch { }
                        }
                        SaveWarmingState();
                    }
                });
            }
            catch { }
        }

        private static void StopWarmingTimer(string phoneNumber, bool saveRemaining = true, bool closeBrowser = true)
        {
            if (_warmingCtsByPhone.TryGetValue(phoneNumber, out var cts))
            {
                try { cts.Cancel(); } catch { }
                _warmingCtsByPhone.Remove(phoneNumber);
            }
            if (saveRemaining)
            {
                if (_warmingEndsByPhone.TryGetValue(phoneNumber, out var ends))
                {
                    var left = ends - DateTime.UtcNow;
                    if (left < TimeSpan.Zero) left = TimeSpan.Zero;
                    _warmingRemainingByPhone[phoneNumber] = left;
                }
            }
            _warmingEndsByPhone.Remove(phoneNumber);
            SaveWarmingState();

            // Закрываем браузер при принудительной остановке прогрева (если нужно)
            if (closeBrowser)
            {
                _ = Task.Run(async () =>
                {
                    try 
                    { 
                        Console.WriteLine($"[WARMING] 🛑 Закрываю браузер для принудительно остановленного прогрева номера {phoneNumber}");
                        
                        // Получаем директорию сессии для этого номера
                        if (_sessionDirByPhone.TryGetValue(phoneNumber, out var sessionDir) && !string.IsNullOrEmpty(sessionDir))
                        {
                            Console.WriteLine($"[WARMING] 📁 Найдена директория сессии: {sessionDir}");
                            
                            // Пытаемся подключиться к браузеру и закрыть его
                            try
                            {
                                await using var cdp = await MaxWebAutomation.ConnectAsync(sessionDir, "web.max.ru");
                                await cdp.CloseBrowserAsync();
                                Console.WriteLine($"[WARMING] ✅ Браузер для номера {phoneNumber} успешно закрыт при остановке");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[WARMING] ⚠️ Ошибка при закрытии браузера для номера {phoneNumber} при остановке: {ex.Message}");
                            }
                            
                            // Удаляем директорию сессии
                            _sessionDirByPhone.Remove(phoneNumber);
                            Console.WriteLine($"[WARMING] 🗑️ Директория сессии для номера {phoneNumber} удалена при остановке");
                        }
                        else
                        {
                            Console.WriteLine($"[WARMING] ⚠️ Директория сессии для номера {phoneNumber} не найдена при остановке");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[WARMING] ❌ Ошибка при закрытии браузера для номера {phoneNumber} при остановке: {ex.Message}");
                    }
                });
            }
        }

        private static void AddWarmingHours(string phoneNumber, int hours, long chatId)
        {
            var extension = TimeSpan.FromHours(hours);
            if (_warmingCtsByPhone.ContainsKey(phoneNumber) && _warmingEndsByPhone.TryGetValue(phoneNumber, out var ends))
            {
                var remaining = ends - DateTime.UtcNow;
                if (remaining < TimeSpan.Zero) remaining = TimeSpan.Zero;
                StartWarmingTimer(phoneNumber, chatId, remaining + extension);
            }
            else
            {
                // Если прогрев не запущен, просто накапливаем оплаченные часы
                if (_warmingRemainingByPhone.TryGetValue(phoneNumber, out var remain) && remain > TimeSpan.Zero)
                {
                    _warmingRemainingByPhone[phoneNumber] = remain + extension;
                }
                else
                {
                    _warmingRemainingByPhone[phoneNumber] = extension;
                }
                SaveWarmingState();
            }
        }

        private static string GetWarmingStatus(string phoneNumber)
        {
            if (_warmingCtsByPhone.ContainsKey(phoneNumber) && _warmingEndsByPhone.TryGetValue(phoneNumber, out var ends))
            {
                var left = ends - DateTime.UtcNow;
                if (left < TimeSpan.Zero) left = TimeSpan.Zero;
                return $"⏳ Осталось: {left.Hours:D2}:{left.Minutes:D2}:{left.Seconds:D2}";
            }
            if (_warmingRemainingByPhone.TryGetValue(phoneNumber, out var remain) && remain > TimeSpan.Zero)
            {
                return $"⏸ На паузе: {remain.Hours:D2}:{remain.Minutes:D2}:{remain.Seconds:D2}";
            }
            return "⏸ Прогрев не запущен";
        }

        private static string GetWarmingType(string phone)
        {
            return _warmingTypeByPhone.TryGetValue(phone, out var type) ? type : "Max";
        }

        private static string FormatWarmingText(string phoneNumber)
        {
            var isRunning = _warmingCtsByPhone.ContainsKey(phoneNumber) && _warmingEndsByPhone.ContainsKey(phoneNumber);
            string line1 = isRunning ? "⚙ Прогрев: Работает" : "⚙ Прогрев: Не запущен";

            string line2;
            if (isRunning)
            {
                var ends = _warmingEndsByPhone[phoneNumber];
                var left = ends - DateTime.UtcNow;
                if (left < TimeSpan.Zero) left = TimeSpan.Zero;
                line2 = $"📊 Статус: Осталось {left.Hours:D2}:{left.Minutes:D2}:{left.Seconds:D2}";
            }
            else if (_warmingRemainingByPhone.TryGetValue(phoneNumber, out var remain) && remain > TimeSpan.Zero)
            {
                line2 = $"📊 Статус: Осталось {remain.Hours:D2}:{remain.Minutes:D2}:{remain.Seconds:D2}";
            }
            else
            {
                line2 = "📊 Статус: Не активен";
            }
            return line1 + "\n" + line2;
        }

        private static void SaveWarmingState()
        {
            try
            {
                lock (_warmingStateLock)
                {
                    var running = new Dictionary<string, double>();
                    foreach (var kv in _warmingEndsByPhone)
                    {
                        var left = kv.Value - DateTime.UtcNow;
                        if (left > TimeSpan.Zero)
                            running[kv.Key] = left.TotalSeconds;
                    }

                    var paused = new Dictionary<string, double>();
                    foreach (var kv in _warmingRemainingByPhone)
                    {
                        if (kv.Value > TimeSpan.Zero)
                            paused[kv.Key] = kv.Value.TotalSeconds;
                    }

                    var intervals = new Dictionary<string, int[]>();
                    foreach (var kv in _warmingIntervalsByPhone)
                    {
                        intervals[kv.Key] = new[] { kv.Value.Min, kv.Value.Max };
                    }

                    var state = new PersistedWarmingState { Running = running, Paused = paused, Intervals = intervals };
                    var json = JsonConvert.SerializeObject(state);
                    System.IO.File.WriteAllText(WarmingStateFile, json);
                }
            }
            catch { }
        }

        private static void LoadWarmingState()
        {
            try
            {
                if (!System.IO.File.Exists(WarmingStateFile)) return;
                var json = System.IO.File.ReadAllText(WarmingStateFile);
                var state = JsonConvert.DeserializeObject<PersistedWarmingState>(json);
                if (state == null) return;

                if (state.Paused != null)
                {
                    foreach (var kv in state.Paused)
                    {
                        _warmingRemainingByPhone[kv.Key] = TimeSpan.FromSeconds(kv.Value);
                    }
                }

                if (state.Intervals != null)
                {
                    foreach (var kv in state.Intervals)
                    {
                        var arr = kv.Value;
                        if (arr != null && arr.Length == 2)
                        {
                            _warmingIntervalsByPhone[kv.Key] = (arr[0], arr[1]);
                        }
                    }
                }

                if (state.Running != null)
                {
                    foreach (var kv in state.Running)
                    {
                        var remain = TimeSpan.FromSeconds(kv.Value);
                        if (remain > TimeSpan.Zero)
                        {
                            StartWarmingTimer(kv.Key, 0, remain);
                        }
                    }
                }
            }
            catch { }
        }

        private static (int Min, int Max) GetMessageInterval(string phone)
        {
            if (_warmingIntervalsByPhone.TryGetValue(phone, out var interval))
                return interval;
            return (30, 120);
        }

        private static void SetMessageInterval(string phone, int min, int max)
        {
            _warmingIntervalsByPhone[phone] = (min, max);
            SaveWarmingState();
        }

        private static bool TryParseInterval(string text, out int min, out int max)
        {
            min = max = 0;
            var parts = text.Replace(" ", "").Split('-', '–');
            if (parts.Length == 2 &&
                int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var a) &&
                int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var b))
            {
                min = Math.Min(a, b);
                max = Math.Max(a, b);
                return min > 0 && max >= min;
            }
            return false;
        }
        
        private static async Task SendRandomMessageAsync(MaxWebAutomation cdp)
        {
            try
            {
                Console.WriteLine("[MAX] Начинаю ввод случайного сообщения...");
                
                // Читаем шаблоны сообщений из файла
                var messageTemplates = await ReadMessageTemplatesAsync();
                if (messageTemplates.Count == 0)
                {
                    Console.WriteLine("[MAX] ⚠️ Шаблоны сообщений не найдены, используем стандартное сообщение");
                    messageTemplates = new List<string> { "Привет! Как дела?" };
                }
                
                // Выбираем случайное сообщение
                var randomMessage = messageTemplates[new Random().Next(messageTemplates.Count)];
                Console.WriteLine($"[MAX] Выбрано сообщение: {randomMessage}");
                
                // Ищем поле для ввода сообщения
                Console.WriteLine("[MAX] Отправляю JavaScript для поиска поля сообщения...");
                var messageResult = await cdp.SendAsync("Runtime.evaluate", new JObject
                {
                    ["expression"] = $@"
                        (function() {{
                            var messageInput = document.querySelector('div.contenteditable.svelte-1frs97c[contenteditable][role=""textbox""][placeholder=""Сообщение""]') ||
                                             document.querySelector('div[contenteditable][role=""textbox""][placeholder=""Сообщение""][data-lexical-editor=""true""]') ||
                                             document.querySelector('div[contenteditable][role=""textbox""][placeholder=""Сообщение""]') ||
                                             document.querySelector('div[contenteditable][role=""textbox""]') ||
                                             document.querySelector('div.contenteditable') ||
                                             document.querySelector('div[data-lexical-editor=""true""]');
                            
                            if (messageInput) {{
                                // Очищаем поле и вводим сообщение
                                messageInput.innerHTML = '';
                                messageInput.textContent = '{randomMessage}';
                                            
                                // Создаем события для активации поля
                                messageInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
                                messageInput.dispatchEvent(new Event('change', {{ bubbles: true }}));
                                messageInput.dispatchEvent(new Event('keyup', {{ bubbles: true }}));
                                messageInput.dispatchEvent(new Event('paste', {{ bubbles: true }}));
                                            
                                // Фокусируемся на поле
                                messageInput.focus();
                                            
                                // Дополнительно симулируем ввод текста
                                var textEvent = new InputEvent('input', {{ 
                                    bubbles: true, 
                                    cancelable: true,
                                    inputType: 'insertText',
                                    data: '{randomMessage}'
                                }});
                                messageInput.dispatchEvent(textEvent);
                                            
                                // Принудительно обновляем содержимое
                                messageInput.innerHTML = '<p class=""paragraph"">{randomMessage}</p>';
                                            
                                // Ждем 2 секунды и нажимаем кнопку отправки
                                setTimeout(function() {{
                                    var sendButton = document.querySelector('button[aria-label=""Отправить сообщение""]') ||
                                                   document.querySelector('button.button[aria-label*=""Отправить""]') ||
                                                   document.querySelector('button.button svg[href=""#icon_send_24""]').closest('button');
                                    
                                    if (sendButton) {{
                                        sendButton.click();
                                    }}
                                }}, 2000);
                                
                                return {{ success: true, message: messageInput.textContent }};
                            }} else {{
                                return {{ success: false, error: 'Поле для сообщения не найдено' }};
                            }}
                        }})()
                    ",
                    ["returnByValue"] = true
                });
                Console.WriteLine("[MAX] JavaScript для поиска поля сообщения отправлен");
                
                // Проверяем результат ввода сообщения
                try
                {
                    var mr1 = messageResult["result"] as JObject;
                    var mr2 = mr1 != null ? mr1["result"] as JObject : null;
                    var mval = mr2 != null ? mr2["value"] : null;
                    
                    if (mval != null && mval.Type == JTokenType.Object)
                    {
                        var messageSuccess = mval["success"]?.Value<bool>() ?? false;
                        if (messageSuccess)
                        {
                            var message = mval["message"]?.Value<string>();
                            Console.WriteLine($"[MAX] ✅ Сообщение успешно введено: {message}");
                        }
                        else
                        {
                            var error = mval["error"]?.Value<string>();
                            Console.WriteLine($"[MAX] ❌ Ошибка ввода сообщения: {error}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[MAX] ❌ Ошибка при обработке результата ввода сообщения: {ex.Message}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MAX] ❌ Ошибка при вводе сообщения: {ex.Message}");
            }
        }
        
        private static async Task<List<string>> ReadMessageTemplatesAsync()
        {
            try
            {
                var templates = new List<string>();
                var filePath = Path.Combine(Directory.GetCurrentDirectory(), "message_templates.txt");
                
                if (System.IO.File.Exists(filePath))
                {
                    var lines = await System.IO.File.ReadAllLinesAsync(filePath);
                    foreach (var line in lines)
                    {
                        var trimmedLine = line.Trim();
                        if (!string.IsNullOrEmpty(trimmedLine))
                        {
                            templates.Add(trimmedLine);
                        }
                    }
                    Console.WriteLine($"[MAX] Загружено {templates.Count} шаблонов сообщений");
                }
                else
                {
                    Console.WriteLine("[MAX] ⚠️ Файл message_templates.txt не найден");
                }
                
                return templates;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MAX] ❌ Ошибка при чтении шаблонов сообщений: {ex.Message}");
                return new List<string>();
            }
        }
        
        private static async Task<bool> CheckAndHandleCaptchaAsync(MaxWebAutomation cdp, string context)
        {
            try
            {
                Console.WriteLine($"[MAX] Проверяю капчу {context}...");
                
                var captchaCheck = await cdp.SendAsync("Runtime.evaluate", new JObject
                {
                    ["expression"] = @"
                        (function() {
                            try {
                                console.log('=== ПОИСК КАПЧИ ===');
                                
                                // Ищем модальное окно с капчей по разным селекторам
                                var captchaSelectors = [
                                    '.modal',
                                    '[class*=""modal""]',
                                    '[class*=""captcha""]',
                                    '[class*=""robot""]',
                                    'div[class*=""challenge""]',
                                    'div[class*=""warp""]'
                                ];
                                
                                var captchaModal = null;
                                for (var i = 0; i < captchaSelectors.length; i++) {
                                    captchaModal = document.querySelector(captchaSelectors[i]);
                                    if (captchaModal) {
                                        console.log('Найден модал капчи:', captchaSelectors[i]);
                                        break;
                                    }
                                }
                                
                                if (captchaModal) {
                                    console.log('Модал капчи найден, ищу кнопку...');
                                    
                                    // Ищем кнопку 'Продолжить' по разным селекторам
                                    var buttonSelectors = [
                                        'button.start',
                                        'button[class*=""start""]',
                                        'button[class*=""continue""]',
                                        'button[class*=""verify""]',
                                        'button[class*=""btn""]'
                                    ];
                                    
                                    var continueButton = null;
                                    for (var j = 0; j < buttonSelectors.length; j++) {
                                        try {
                                            continueButton = captchaModal.querySelector(buttonSelectors[j]);
                                            if (continueButton) {
                                                console.log('Кнопка найдена по селектору:', buttonSelectors[j]);
                                                break;
                                            }
                                        } catch(e) {
                                            console.log('Ошибка селектора:', buttonSelectors[j], e.message);
                                        }
                                    }
                                    
                                    if (continueButton) {
                                        console.log('Кнопка продолжения найдена, нажимаю...');
                                        continueButton.click();
                                        return { found: true, clicked: true, buttonText: continueButton.textContent };
                                    } else {
                                        console.log('Кнопка не найдена в модале');
                                        return { found: true, clicked: false, error: 'Кнопка не найдена в модале' };
                                    }
                                }
                                
                                // Альтернативный поиск по тексту всех кнопок на странице
                                console.log('Ищу кнопки по тексту...');
                                var allButtons = Array.from(document.querySelectorAll('button'));
                                var continueBtn = allButtons.find(btn => {
                                    var text = btn.textContent || '';
                                    return text.includes('Продолжить') || 
                                           text.includes('Continue') ||
                                           text.includes('Проверить') ||
                                           text.includes('Verify') ||
                                           text.includes('Подтвердить') ||
                                           text.includes('Confirm') ||
                                           text.includes('Начать') ||
                                           text.includes('Start');
                                });
                                
                                if (continueBtn) {
                                    console.log('Кнопка продолжения найдена по тексту:', continueBtn.textContent);
                                    continueBtn.click();
                                    return { found: true, clicked: true, buttonText: continueBtn.textContent };
                                }
                                
                                console.log('Капча не найдена');
                                return { found: false, clicked: false };
                            } catch(e) {
                                console.log('Ошибка поиска капчи:', e.message);
                                return { error: e.message };
                            }
                        })()
                    ",
                    ["returnByValue"] = true
                });
                
                if (captchaCheck?["result"]?["result"]?["value"] != null)
                {
                    var captchaResult = captchaCheck["result"]["result"]["value"];
                    if (captchaResult["found"]?.Value<bool>() == true && captchaResult["clicked"]?.Value<bool>() == true)
                    {
                        Console.WriteLine($"[MAX] ✅ Капча {context} обработана автоматически! Кнопка: {captchaResult["buttonText"]?.Value<string>()}");
                        return true;
                    }
                    else if (captchaResult["found"]?.Value<bool>() == true && captchaResult["clicked"]?.Value<bool>() == false)
                    {
                        Console.WriteLine($"[MAX] ⚠️ Капча {context} обнаружена, но кнопка не нажата: {captchaResult["error"]?.Value<string>()}");
                        return false;
                    }
                    else
                    {
                        Console.WriteLine($"[MAX] Капча {context} не обнаружена");
                        return false;
                    }
                }
                
                return false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[MAX] Ошибка проверки капчи {context}: {ex.Message}");
                return false;
            }
        }
    }
}
