using System;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace MaxTelegramBot
{
	public sealed class MaxWebAutomation : IAsyncDisposable
	{
		private readonly ClientWebSocket _webSocket = new ClientWebSocket();
		private readonly CancellationTokenSource _cts = new CancellationTokenSource();
		private int _messageId;

		public static async Task<MaxWebAutomation> ConnectAsync(string userDataDir, string pageUrlContains, int timeoutMs = 15000, JObject? additionalSettings = null)
		{
			var portFile = Path.Combine(userDataDir, "DevToolsActivePort");
			var sw = Stopwatch.StartNew();
			while (!File.Exists(portFile))
			{
				await Task.Delay(200);
				if (sw.ElapsedMilliseconds > timeoutMs)
					throw new Exception("DevToolsActivePort not found. Make sure Chrome started with --remote-debugging-port=0");
			}

			var lines = await File.ReadAllLinesAsync(portFile);
			if (lines.Length == 0)
				throw new Exception("DevToolsActivePort file is empty");
			if (!int.TryParse(lines[0], out var port))
				throw new Exception("Invalid DevTools port value");

			string wsDebuggerUrl;
			using (var http = new HttpClient())
			{
				var json = await http.GetStringAsync($"http://127.0.0.1:{port}/json");
				var arr = JArray.Parse(json);
				wsDebuggerUrl = (arr.FirstOrDefault(x => x["type"]?.ToString() == "page" && (x["url"]?.ToString()?.Contains(pageUrlContains) ?? false))?
					["webSocketDebuggerUrl"])?.ToString();
				if (string.IsNullOrEmpty(wsDebuggerUrl))
				{
					wsDebuggerUrl = (arr.FirstOrDefault(x => x["type"]?.ToString() == "page")?
						["webSocketDebuggerUrl"])?.ToString();
				}
			}
			if (string.IsNullOrEmpty(wsDebuggerUrl))
				throw new Exception("No page target found for DevTools");

			var client = new MaxWebAutomation();
			await client._webSocket.ConnectAsync(new Uri(wsDebuggerUrl), client._cts.Token);
			await client.EnableBasicDomainsAsync();
			
			// Применяем оптимизации для экономии ресурсов
			if (additionalSettings != null)
			{
				await client.ApplyOptimizationsAsync(additionalSettings);
			}
			
			return client;
		}

		public async Task EnableBasicDomainsAsync()
		{
			await SendAsync("Page.enable");
			await SendAsync("Runtime.enable");
			await SendAsync("DOM.enable");
			await SendAsync("Network.enable");
		}

		public async Task SetUserAgentAsync(string userAgent)
		{
			await SendAsync("Network.setUserAgentOverride", new JObject
			{
				["userAgent"] = userAgent
			});
		}

		private static readonly string[] _userAgentTemplates = {
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
		};

		public async Task SetRandomUserAgentAsync()
		{
			var random = new Random();
			var template = _userAgentTemplates[random.Next(_userAgentTemplates.Length)];
			// Добавляем небольшую вариацию в версию Chrome
			var chromeVersion = random.Next(118, 124);
			var patchVersion = random.Next(0, 10);
			var userAgent = template.Replace("Chrome/120.0.0.0", $"Chrome/{chromeVersion}.0.{patchVersion}.0");
			await SetUserAgentAsync(userAgent);
		}

		public async Task ApplyOptimizationsAsync(JObject settings)
		{
			try
			{
				// Отключаем ненужные домены для экономии ресурсов
				await SendAsync("Page.setBypassCSP", new JObject { ["enabled"] = true });
				await SendAsync("Page.setLifecycleEventsEnabled", new JObject { ["enabled"] = false });
				await SendAsync("Page.setInterceptFileChooserDialog", new JObject { ["enabled"] = false });
				
				// Отключаем сетевые события
				await SendAsync("Network.setBypassServiceWorker", new JObject { ["bypass"] = true });
				await SendAsync("Network.setCacheDisabled", new JObject { ["cacheDisabled"] = true });
				
				// Отключаем логирование
				await SendAsync("Runtime.setAsyncCallStackDepth", new JObject { ["maxDepth"] = 0 });
				
				// Устанавливаем ограничения на память
				await SendAsync("Runtime.setMaxCallStackSize", new JObject { ["size"] = 100 });
				
				Console.WriteLine("[MAX] ✅ Оптимизации применены");
			}
			catch (Exception ex)
			{
				Console.WriteLine($"[MAX] ⚠️ Ошибка применения оптимизаций: {ex.Message}");
                        }
                }

                private const string DeepQueryScript = @"function __maxFind(root, sel){try{var el=root.querySelector(sel);if(el) return el;}catch(e){}var nodes=root.querySelectorAll('*');for(var i=0;i<nodes.length;i++){var node=nodes[i];try{var cd=node.contentDocument;if(cd){var res=__maxFind(cd, sel);if(res) return res;}}catch(e){}var sr=node.shadowRoot;if(sr){var res2=__maxFind(sr, sel);if(res2) return res2;}}return null;}";

                private static string BuildDeepQueryExpression(string selector, string action, string? value = null)
                {
                        var selEsc = EscapeJs(selector);
                        if (value != null)
                        {
                                var valEsc = EscapeJs(value);
                                return "(function(sel,val){" + DeepQueryScript + " var el=__maxFind(document, sel); " + action + "})('" + selEsc + "','" + valEsc + "')";
                        }
                        return "(function(sel){" + DeepQueryScript + " var el=__maxFind(document, sel); " + action + "})('" + selEsc + "')";
                }

                public async Task FocusSelectorAsync(string cssSelector)
                {
                        var expr = $"document.querySelector('{EscapeJs(cssSelector)}')?.focus()";
                        await SendAsync("Runtime.evaluate", new JObject
                        {
				["expression"] = expr,
				["awaitPromise"] = false
			});
		}

                public async Task ClearInputAsync(string cssSelector)
                {
                        var expr = "(function(){var el=document.querySelector('" + EscapeJs(cssSelector) + "');" +
                                   " if(el){if('value' in el){el.value='';}else{el.textContent='';}" +
                                   " el.dispatchEvent(new Event('input',{bubbles:true}));}})()";
                        await SendAsync("Runtime.evaluate", new JObject
                        {
                                ["expression"] = expr,
                                ["awaitPromise"] = false
                        });
                }

                public async Task<bool> SetInputValueAsync(string cssSelector, string value)
                {
                        var expr = BuildDeepQueryExpression(cssSelector,
                                "if(el){if('value' in el){el.value=val;}else{el.textContent=val;} el.dispatchEvent(new Event('input',{bubbles:true})); return true;} return false;",
                                value);
                        var resp = await SendAsync("Runtime.evaluate", new JObject
                        {
                                ["expression"] = expr,
                                ["awaitPromise"] = false,
                                ["returnByValue"] = true
                        });
                        return resp?["result"]?["value"]?.Value<bool?>() == true;
                }

                public async Task<string?> GetInputValueAsync(string cssSelector)
                {
                        var expr = "(function(){var el=document.querySelector('" + EscapeJs(cssSelector) + "');" +
                                   " if(!el) return null;" +
                                   " return ('value' in el) ? el.value : (el.textContent||null);})()";
                        var resp = await SendAsync("Runtime.evaluate", new JObject
                        {
                                ["expression"] = expr,
                                ["awaitPromise"] = true,
                                ["returnByValue"] = true
                        });
                        return resp?["result"]?["value"]?.Value<string>();
                }

                public async Task ClearInputAsync()
                {
                        await SendAsync("Input.clear", new JObject());
                }

		public async Task TypeTextAsync(string text)
		{
			await SendAsync("Input.insertText", new JObject
			{
				["text"] = text
			});
		}

                public async Task<bool> ClickSelectorAsync(string cssSelector)
                {
                        var expr = BuildDeepQueryExpression(cssSelector,
                                "if(el){el.click(); return true;} return false;");
                        var resp = await SendAsync("Runtime.evaluate", new JObject
                        {
                                ["expression"] = expr,
                                ["awaitPromise"] = true,
                                ["returnByValue"] = true
                        });
                        return resp?["result"]?["value"]?.Value<bool?>() == true;
                }

                public async Task<bool> ClickSelectorByMouseAsync(string cssSelector)
                {
                        var expr = "(function(sel){var el=document.querySelector(sel);" +
                                "if(!el) return null;" +
                                "el.scrollIntoView({behavior:'instant',block:'center',inline:'center'});" +
                                "var r=el.getBoundingClientRect();" +
                                "return {x:r.left+r.width/2,y:r.top+r.height/2};})(" +
                                JsonConvert.SerializeObject(cssSelector) + ")";
                        var resp = await SendAsync("Runtime.evaluate", new JObject
                        {
                                ["expression"] = expr,
                                ["awaitPromise"] = true,
                                ["returnByValue"] = true
                        });
                        var val = resp?["result"]?["value"];
                        if (val == null || val.Type != JTokenType.Object)
                                return false;
                        var x = val["x"]?.Value<double?>();
                        var y = val["y"]?.Value<double?>();
                        if (x == null || y == null)
                                return false;
                        await SendAsync("Input.dispatchMouseEvent", new JObject
                        {
                                ["type"] = "mouseMoved",
                                ["x"] = x,
                                ["y"] = y,
                                ["button"] = "none"
                        });
                        await SendAsync("Input.dispatchMouseEvent", new JObject
                        {
                                ["type"] = "mousePressed",
                                ["x"] = x,
                                ["y"] = y,
                                ["button"] = "left",
                                ["clickCount"] = 1
                        });
                        await SendAsync("Input.dispatchMouseEvent", new JObject
                        {
                                ["type"] = "mouseReleased",
                                ["x"] = x,
                                ["y"] = y,
                                ["button"] = "left",
                                ["clickCount"] = 1
                        });
                        return true;
                }

                public async Task<bool> WaitForSelectorAsync(string cssSelector, int timeoutMs = 15000, int pollMs = 250)
                {
                        var sw = Stopwatch.StartNew();
                        while (sw.ElapsedMilliseconds < timeoutMs)
                        {
                                var expr = BuildDeepQueryExpression(cssSelector, "return el!=null;");
                                var resp = await SendAsync("Runtime.evaluate", new JObject
                                {
                                        ["expression"] = expr,
                                        ["awaitPromise"] = false,
                                        ["returnByValue"] = true
                                });
                                var result = resp?["result"]?["value"]?.Value<bool?>();
                                if (result == true) return true;
                                await Task.Delay(pollMs);
                        }
                        return false;
                }

                public async Task<bool> WaitForXPathAsync(string xpath, int timeoutMs = 15000, int pollMs = 250)
                {
                        var sw = Stopwatch.StartNew();
                        var escaped = EscapeJs(xpath);
                        while (sw.ElapsedMilliseconds < timeoutMs)
                        {
                                var expr = "(function(p){try{var n=document.evaluate(p,document,null,XPathResult.FIRST_ORDERED_NODE_TYPE,null).singleNodeValue;return n!=null;}catch(e){return false;}})('" + escaped + "')";
                                var resp = await SendAsync("Runtime.evaluate", new JObject
                                {
                                        ["expression"] = expr,
                                        ["awaitPromise"] = false,
                                        ["returnByValue"] = true
                                });
                                var result = resp?["result"]?["value"]?.Value<bool?>();
                                if (result == true) return true;
                                await Task.Delay(pollMs);
                        }
                        return false;
                }

		public async Task<bool> WaitForBodyTextContainsAsync(string substring, int timeoutMs = 15000, int pollMs = 250)
		{
			var sw = Stopwatch.StartNew();
			while (sw.ElapsedMilliseconds < timeoutMs)
			{
				var resp = await SendAsync("Runtime.evaluate", new JObject
				{
					["expression"] = "(function(s){var b=document.body; if(!b) return false; var t=(b.innerText||'').toLowerCase(); return t.indexOf(s.toLowerCase())>=0;})(" + JsonConvert.SerializeObject(substring) + ")",
					["awaitPromise"] = true,
					["returnByValue"] = true
				});
				var result = resp?["result"]?["value"]?.Value<bool?>();
				if (result == true) return true;
				await Task.Delay(pollMs);
			}
			return false;
		}

                public async Task<bool> TypeIntoFirstVisibleTextInputAsync(string text)
                {
                        var expr = "(function(txt){"+
                                      "function vis(el){var s=getComputedStyle(el);if(s.display==='none'||s.visibility==='hidden')return false;var r=el.getBoundingClientRect();return r.width>0&&r.height>0;}"+
                                      "function trySet(el){try{el.focus();if('value' in el){el.value='';el.dispatchEvent(new Event('input',{bubbles:true}));el.value=txt;el.dispatchEvent(new Event('input',{bubbles:true}));el.dispatchEvent(new Event('change',{bubbles:true}));return true;}var ce=el.getAttribute&&el.getAttribute('contenteditable');if(ce&&ce.toLowerCase()==='true'){document.execCommand('selectAll',false,null);document.execCommand('insertText',false,txt);return true;}}catch(e){}return false;}"+
                                      "function search(root){var sels=['input','textarea','[contenteditable\\x3d\"true\"]'];for(var si=0;si<sels.length;si++){var list=root.querySelectorAll?root.querySelectorAll(sels[si]):[];for(var i=0;i<list.length;i++){var el=list[i];var t=(el.getAttribute('type')||'').toLowerCase();if(sels[si]!=='input'||t===''||t==='text'||t==='tel'||t==='number'||t==='search'){if(vis(el)&&trySet(el))return true;}}}var all=root.querySelectorAll?root.querySelectorAll('*'):[];for(var j=0;j<all.length;j++){var e=all[j];if(e.shadowRoot&&search(e.shadowRoot))return true;}return false;}"+
                                      "return search(document);"+
                                      "})(" + JsonConvert.SerializeObject(text) + ")";
                        var resp = await SendAsync("Runtime.evaluate", new JObject
                        {
                                ["expression"] = expr,
                                ["awaitPromise"] = true,
                                ["returnByValue"] = true
                        });
                        return resp?["result"]?["value"]?.Value<bool?>() == true;
                }

                public async Task<bool> TypeIntoActiveElementAsync(string text)
                {
                        var expr = "(function(txt){var el=document.activeElement;if(!el) return false;try{if('value' in el){el.value='';el.dispatchEvent(new Event('input',{bubbles:true}));el.value=txt;el.dispatchEvent(new Event('input',{bubbles:true}));el.dispatchEvent(new Event('change',{bubbles:true}));return true;}var ce=el.getAttribute&&el.getAttribute('contenteditable');if(ce&&ce.toLowerCase()==='true'){document.execCommand('selectAll',false,null);document.execCommand('insertText',false,txt);return true;}}catch(e){}return false;})(" + JsonConvert.SerializeObject(text) + ")";
                        var resp = await SendAsync("Runtime.evaluate", new JObject
                        {
                                ["expression"] = expr,
                                ["awaitPromise"] = true,
                                ["returnByValue"] = true
                        });
                        return resp?["result"]?["value"]?.Value<bool?>() == true;
                }

                public async Task<bool> ClickButtonByTextAsync(string containsText)
                {
                        var expr = "(function(t){t=t.toLowerCase();var btns=Array.from(document.querySelectorAll(\"button,[role='button']\"));for(var i=0;i<btns.length;i++){var el=btns[i];var txt=(el.textContent||'').trim().toLowerCase();if(txt.indexOf(t)>=0){el.click();return true;}}return false;})(" + JsonConvert.SerializeObject(containsText) + ")";
                        var resp = await SendAsync("Runtime.evaluate", new JObject
                        {
                                ["expression"] = expr,
                                ["awaitPromise"] = true,
                                ["returnByValue"] = true
                        });
                        return resp?["result"]?["value"]?.Value<bool?>() == true;
                }

                public async Task<string?> GetTextBySelectorAsync(string cssSelector)
                {
                        var expr = BuildDeepQueryExpression(cssSelector,
                                "if(!el) return '';" +
                                "var doc=el.ownerDocument;" +
                                "var matches=doc.querySelectorAll(sel);" +
                                "var res='';" +
                                "for(var i=0;i<matches.length;i++){var t=matches[i].textContent; if(t) res+=t.trim();}" +
                                "return res;");
                        var resp = await SendAsync("Runtime.evaluate", new JObject
                        {
                                ["expression"] = expr,
                                ["awaitPromise"] = true,
                                ["returnByValue"] = true
                        });
                        return resp?["result"]?["value"]?.Value<string>();
                }

                public async Task<string?> GetBodyTextAsync()
                {
                        var resp = await SendAsync("Runtime.evaluate", new JObject
                        {
                                ["expression"] = "document.body ? document.body.innerText : ''",
                                ["awaitPromise"] = true,
                                ["returnByValue"] = true
                        });
                        return resp?["result"]?["value"]?.Value<string>();
                }

		public async Task<bool> FillOtpInputsAsync(string digits)
		{
			var expr = "(function(code){"+
				"var selectors=['div.code input','input.digit','input[type=number].digit','input[type=number]'];"+
				"var inputs=null;"+
				"for(var i=0;i<selectors.length;i++){var list=document.querySelectorAll(selectors[i]); if(list&&list.length){inputs=list; if(list.length>=code.length) break;}}"+
				"if(!inputs||inputs.length===0) return false;"+
				"for(var i=0;i<code.length && i<inputs.length;i++){var el=inputs[i]; try{el.focus(); el.value=''; el.dispatchEvent(new Event('input',{bubbles:true})); el.value=code[i]; el.dispatchEvent(new Event('input',{bubbles:true})); el.dispatchEvent(new Event('change',{bubbles:true}));}catch(e){}}"+
				"return true;"+
			"})(" + JsonConvert.SerializeObject(digits) + ")";
			var resp = await SendAsync("Runtime.evaluate", new JObject
			{
				["expression"] = expr,
				["awaitPromise"] = true,
				["returnByValue"] = true
			});
			return resp?["result"]?["value"]?.Value<bool?>() == true;
		}

		public async Task<bool> SubmitFormBySelectorAsync(string formSelector)
		{
			var expr = "(function(sel){var f=document.querySelector(sel); if(!f) return false; if(typeof f.requestSubmit==='function'){f.requestSubmit(); return true;} try{f.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true})); if(typeof f.submit==='function') f.submit(); return true;}catch(e){return false;}})(" + JsonConvert.SerializeObject(formSelector) + ")";
			var resp = await SendAsync("Runtime.evaluate", new JObject
			{
				["expression"] = expr,
				["awaitPromise"] = true,
				["returnByValue"] = true
			});
			return resp?["result"]?["value"]?.Value<bool?>() == true;
		}

		public async Task PressEnterAsync()
		{
			var expr = "(function(){function fire(target,type){var e=new KeyboardEvent(type,{key:'Enter',code:'Enter',keyCode:13,which:13,bubbles:true}); target.dispatchEvent(e);} var a=document.activeElement||document.body; if(a){fire(a,'keydown');fire(a,'keyup');} fire(document,'keydown'); fire(document,'keyup'); return true;})()";
			await SendAsync("Runtime.evaluate", new JObject
			{
				["expression"] = expr,
				["awaitPromise"] = true,
				["returnByValue"] = true
			});
		}

		public async Task<JToken> SendAsync(string method, JObject? parameters = null)
		{
			var id = Interlocked.Increment(ref _messageId);
			var obj = new JObject
			{
				["id"] = id,
				["method"] = method
			};
			if (parameters != null)
				obj["params"] = parameters;

			var json = obj.ToString(Formatting.None);
			var buffer = Encoding.UTF8.GetBytes(json);
			await _webSocket.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, _cts.Token);

			// Read until we get the matching id response, ignore events
			var readBuffer = new byte[1 << 16];
			while (true)
			{
				var ms = new MemoryStream();
				WebSocketReceiveResult result;
				do
				{
					result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(readBuffer), _cts.Token);
					ms.Write(readBuffer, 0, result.Count);
				}
				while (!result.EndOfMessage);
				var text = Encoding.UTF8.GetString(ms.ToArray());
				var jt = JToken.Parse(text);
				if (jt["id"]?.Value<int>() == id)
					return jt;
				// else it's an event; continue reading
			}
		}

		private static string EscapeJs(string s)
		{
			return s.Replace("\\", "\\\\").Replace("'", "\\'");
		}

		public async Task CloseBrowserAsync()
		{
			try
			{
				// Отправляем команду на закрытие браузера через CDP
				await SendAsync("Browser.close");
			}
			catch
			{
				// Игнорируем ошибки при закрытии
			}
			
			// НЕ убиваем все процессы Chrome - это закрывает браузеры других номеров!
			// Вместо этого полагаемся на CDP команду Browser.close
		}

		public async ValueTask DisposeAsync()
		{
			try { _cts.Cancel(); } catch { }
			try { _webSocket.Dispose(); } catch { }
			await Task.CompletedTask;
		}
	}
} 
