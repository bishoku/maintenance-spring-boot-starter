> Historical scope: HTTP admission/drain assumptions in this report were superseded by [the readiness contract correction](READINESS_CONTRACT.md). Raw outcomes are retained as history, not current acceptance criteria.

# Düzeltme öncesi bulgular

Kaynak referansı: 25e0eba0fe2d8ed7aacbde39d5dcfb49edaf1dae.
İlk gerçek broker senaryosu library kaynakları değiştirilmeden çalıştırıldı.
`baseline-v1/results.json` aynı eski starter JAR'ının SHA-256 bilgisini içerir.
Sonraki senaryolar da bu kurulu JAR'ı kullanır; kaynak değişiklikleri baseline'a dahil değildir.

İlk doğrulanan sonuçlar (Boot 3.5.9 / Java 25.0.2):
- Kafka stop ediliyor; running=false ve paused=false.
- Readiness 503, liveness 200; buna rağmen doğrudan /work isteği 200.
- Bakımda Kafka/Rabbit yeni mesaj tüketmiyor; çıkışta mesajlar tüketiliyor.
- autoStartup=false olan Kafka ve Rabbit listener'ları çıkışta açılıyor.
- Exit hook'u Kafka yeniden çalışmaya başladıktan sonra çağrılıyor.

Önceki incelemede Boot 4.0.2 kaynak derlemesi enableByDefault() kaldırıldığı için başarısızdı.
Bu fixture ayrıca önceden derlenmiş aynı JAR'ın Boot 4 runtime davranışını ölçer.
Tüm senaryoların nihai sonuçları baseline-v1/summary.md ve results.json içinde tutulur.

İlk `baseline/` girişimi test fixture ayar hatası nedeniyle iptal edildi:
Boot aynı endpoint için `enabled` ve `access` ayarlarını birlikte kabul etmiyor.
Fixture yalnızca `access=unrestricted` kullanacak şekilde düzeltildi; bu girişim
ürün davranışı sonucu sayılmamalıdır.

## Tamamlanan başlangıç ölçümleri

- `baseline-v1`: 43 satırın 15'i PASS, 28'i FAIL. Bu sayıya build kontrolleri ve
  iki sürümdeki drain/concurrency çalıştırıcı hataları dahildir; 28 ürün hatası demek değildir.
- Drain/concurrency ilk girişiminde Python fonksiyon ismi `concurrent` modülünü gölgeledi.
  Düzeltilen çalıştırıcıyla `baseline-v2-drain` üzerinde concurrency iki sürümde de PASS.
- `baseline-v2-drain` HTTP drain sonucu, Kafka stop beklemesiyle maskelenen yanlış pozitifti.
  Kuyruk/hook işlemleri kapatılarak izole edilen `baseline-v3-drain` iki sürümde de
  `wait-for-inflight-http: FAIL` verdi. Esas HTTP drain başlangıç sonucu budur.
- Hook timeout sonrası yan etki, başarısız giriş/çıkışta açık kalan trafik,
  giriş tekrarının hatalı no-op olması ve broker hatasının başarı sayılması iki sürümde
  de gözlendi; ayrıntılar JSON dosyalarındadır.
- Opsiyonel reason, eski binary ile iki sürümde de PASS oldu. Resmi göç belgesindeki
  değişiklik kaynak düzeyinde ele alındı; ilk düzeltmede yalnızca JSpecify kullanmak
  Boot 3.5.9'da FAIL verdi (`fixed-v1`). İki anotasyonun birlikte kullanımı doğrulandı.

Eski JAR'ın Boot 4 runtime'da açılması, Boot 4 ile kaynak derlenebilirliğiyle aynı şey
değildir. Önceki derleme hatası ve bu runtime sonuçları birbirini çürütmez.
