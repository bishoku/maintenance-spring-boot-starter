> Historical scope: HTTP admission/drain assumptions in this report were superseded by [the readiness contract correction](READINESS_CONTRACT.md). Raw outcomes are retained as history, not current acceptance criteria.

# Düzeltmeler ve doğrulama

GitHub CI workflow dosyaları değiştirilmedi. Test altyapısı ve raporlar `benchmark/`
altında; library değişiklikleri normal `src/` ve `pom.xml` altında tutulur.

## Davranış değişiklikleri

- Spring Boot kaynak tabanı 3.5.9; Actuator `defaultAccess=NONE`. Aynı JAR Boot 3.5.9
  ve 4.0.2 örnek uygulamalarında denenir. Optional reason için legacy + JSpecify köprüsü.
- ENTERING/ACTIVE/EXITING/FAILED/INACTIVE durumları. Kritik hata tamamlanmış bakım
  sayılmaz. HTTP admission kapalı kalır; başarısız komut yeniden denenebilir.
- Hook timeout cancellation ister, sonraki hook'lar kuyruğa alınmaz. Interrupt'a
  uymayan önceki hook bitmeden başka geçişe izin verilmez. Tamamlanma sinyali ile
  Future sonucu arasındaki yarış giderildi.
- Kafka registry stop yerine container pause/resume ve pause acknowledgement.
  Önceden kapalı veya başka nedenle pause edilmiş container'lar korunur.
- Rabbit container stop callback beklenir; yalnızca maintenance'ın durdurduğu
  container'lar yeniden açılır. Kısmi restart sonrası tekrar durdurma ve stop
  callback gelmeden retry/start güvenliği için regresyon testleri bulunur.
- Servlet admission gate yeni isteklere 503 verir, devam eden istekler ve opt-in
  işler lease ile takip edilir. Drain timeout güvenli kapalı durumla sonuçlanır.
- Exit hazırlık hook'ları consumer resume'dan önce çalışır. Başarısız resume'da
  compensation uygulanır; ilk hata ve compensation sonuçları birlikte saklanır.
- İç içe broker hataları ve interrupted drain partialFailure olur; metrikler
  başarısız denemeleri başarı sayacına eklemez. Metrics auto-configuration sırası düzeltildi.
- Süre ayarları doğrulanır; README'deki garantiler gerçek kapsamla sınırlandırıldı.

## Raporların okunması

`baseline-v1`, `baseline-v2-drain`, `baseline-v3-drain` başlangıç ölçümleridir.
[BASELINE.md](BASELINE.md) ölçüm düzeltmelerini ve yanıltıcı sonuçları açıklar.
`fixed-v1` optional reason için Boot 3.5 regresyonunu yakaladı (44/45 PASS).
`fixed-v2` genişletilmiş senaryolarda 63/63 PASS verdi.
`fixed-final` port ön kontrolü TIME_WAIT nedeniyle test başlamadan kesildi; ürün
sonucu değildir. Düzeltilmiş çalıştırıcı ile `fixed-final-v2` kullanılır.

Bir tam E2E koşusu 60 davranış kontrolü (sürüm başına 30) ve 3 build kontrolünden oluşur.
Bunlar 12 ayrı JVM senaryosunda, her Boot sürümü için tekrarlanır. Aynı JAR tüketimi
`consumed_starter_sha256` alanında doğrulanır. Tam koşudan sonraki küçük hata-yolu
sıkılaştırmaları ayrıca hedefli broker E2E koşusu ve tam library testleriyle doğrulanır.

## Kapsam ve açık sınırlar

Bu çalışma pod-local maintenance sağlar. Kubernetes cluster/EndpointSlice yayılımı,
çoklu replika koordinasyonu, restart persistence, broker failover/network partition,
WebFlux, WebSocket/gRPC drain ve kullanıcı uygulamasının kimlik doğrulaması test edilmedi.
Erişim testleri Actuator none/read-only/unrestricted politikasını ölçer; authentication
ve authorization testi değildir. Scheduled işler otomatik durdurulmaz; lease API'sine
entegre edilmelidir. Rabbit forced shutdown/ack/redelivery ayarlarının tüm kombinasyonları
ve exactly-once garantisi bu fixture'ın kapsamı dışındadır. Kafka pause sırasında
rebalance olmamasına yönelik mekanizma doğrulanır; broker topology kaynaklı rebalance
olmayacağı iddia edilmez.

HTTP 200, bakımın tamamlandığını tek başına göstermez: details.phase ve partialFailure
kontrol edilmelidir. Hook'lar retry için idempotent olmalıdır. Kaybolan/hiç gelmeyen Rabbit
stop callback'i otomatik başarıya çevrilmez; müdahale gerekebilir. Native image, her 4.x
sürümü ve Java 21 bu koşularda doğrulanmış sayılmaz.

Javadoc paketlemesi başarılıdır; eksik API yorumlarına ilişkin doclint uyarıları vardır.

## Nihai sonuçlar — 22 Ekim 2026

| Doğrulama | Boot 3.5.9 | Boot 4.0.2 | Kanıt |
|---|---|---|---|
| Java 25.0.2, nihai library kaynak testleri | 81/81 PASS | 81/81 PASS | [library-reviewed-jdk25](library-reviewed-jdk25/results.json) |
| Java 17.0.12, nihai library `clean verify` | 81/81 PASS | 81/81 PASS | [library-reviewed-jdk17](library-reviewed-jdk17/results.json) |
| Java 25.0.2, tam broker/HTTP E2E | 30/30 PASS | 30/30 PASS | [fixed-final-v2](fixed-final-v2/summary.md) |
| Son iki broker hata-yolu düzeltmesinden sonra hedefli E2E | 15/15 PASS | 15/15 PASS | [reviewed-brokers](reviewed-brokers/summary.md) |

Tam E2E raporu build kontrolleriyle 63/63, hedefli son E2E raporu 33/33 PASS içerir.
Nihai library testleri dört Boot/JDK kombinasyonunda toplam 324 başarılı test yürütmesidir.
Java 17 verify ayrıca normal JAR, sources JAR ve Javadoc JAR üretimini doğruladı.
Son hedefli koşu lifecycle, exit_failure, queue_failure, broker_inflight senaryolarını
tekrarladı; diğer HTTP/hook kontratlarının son tam E2E kaydı fixed-final-v2'dir.

İki Boot uygulamasının son hedefli koşuda tükettiği starter JAR SHA-256 aynıdır:
`51bd6b7a94a6f11154e3c970942568e62149a1870963b2e6d97579e028f8ffa1`.
Bu, yalnızca farklı BOM'larla ayrı ayrı source build yapmak yerine aynı binary'nin
her iki runtime'da doğrulandığını gösterir. Tarih/sürüm/sonuçlar JSON raporlarında bulunur.

`git diff --check` temiz; `git diff --exit-code -- .github` başarılı (workflow değişikliği yok).
