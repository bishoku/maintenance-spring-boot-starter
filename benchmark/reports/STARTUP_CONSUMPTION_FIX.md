# Startup bakım modunda erken tüketimin engellenmesi

Tarih: 2026-11-06. Kapsam: yalnızca initial context startup sırasında Kafka/Rabbit
listener'larının bakım çıkışından önce mesaj tüketmesini engellemek.

## Değişiklik

Spring-managed listener factory'leri container oluşturduktan, endpoint ayarları ve
kullanıcı customizer'ları uygulandıktan sonra başlangıç niyeti okunur. Startup bakım
modunda normalde otomatik başlayacak container'ların `autoStartup` değeri ilk lifecycle
başlangıcından önce false yapılır ve startup ownership kaydedilir. ApplicationStartedEvent
sırasında durdurmaya güvenilmez. Registry-managed container'lar genellikle Spring bean'i
olmadığından yalnızca container BeanPostProcessor kullanmak yeterli değildir.

İlk bakım çıkışında yalnızca bu nedenle ertelenmiş container'lar için autoStartup geri
yüklenir ve start çağrılır. Kullanıcının autoStartup=false verdiği container'lar sahiplenilmez.
Sonraki dinamik bakımda mevcut Kafka pause/resume ve Rabbit stop/start davranışı korunur.
Başlatma hatasında startup ownership retry için tutulur. Queue/broker yönetimi kapalıysa
ilgili başlangıç engeli de uygulanmaz.

Factory'ler class proxy ile sarılır; mevcut customizer'lar değiştirilmez, concrete factory
injection korunur. Custom factory sınıfı ve createListenerContainer metodu non-final
olmalıdır; desteklenmeyen final factory sessizce tüketim yapmasına izin verilmeden
context startup'ını başarısız kılar. Factory container'ı dönmeden önce başlatmamalıdır.
Manuel start ve context refresh sonrasında dinamik endpoint kaydı startup garantisinin
dışındadır. Kafka başlangıç bakımı sırasında gruba katılmaz; ilk başarılı çıkışta katılır.

## Test edilen artifact

- Yerel koordinat: `com.bishokudev:maintenance-spring-boot-starter:1.3.1-startup-fix-SNAPSHOT`.
- SHA-256: `3ea3612c95ad7582ed60a3a5002a1fe032e3659f5a360b7c6a1ec13245862959`.
- [Artifact ve kaynak kimliği](startup-fix-candidate/artifact.json).
- Geçici kaynak/POM kopyasında sadece proje sürümü değiştirilerek Boot 3.5.9 / Java 17
  ile build/install edildi. Main kaynaklarının çalışma ağacıyla birebir eşleşmesi kontrol edildi.
- Root POM sürümü 1.3.0 olarak bırakıldı. Yayımlanmış 1.3.0 yerel JAR'ının hash'i hâlâ
  `bc5ffa4e40b304135087540e9693586d543c123d53e32a00e4604249e90b707d`.
- Release/publish yapılmadı. Aynı aday binary farklı Boot BOM'larıyla tüketildi.

## Kaynak matrisi

[Son kaynak doğrulaması](startup-fix-final-source-jdk17/results.json), Java 17:

| Spring Boot | Test | PASS | Hata / atlanan |
|---|---:|---:|---:|
| 3.5.9 | 188 | 188 | 0 / 0 |
| 4.0.2 | 188 | 188 | 0 / 0 |

`clean verify` ayrıca JAR, sources ve Javadoc artifact'larını üretti. Eklenen 11 test:
gerçek factory/endpoint ayarları ve customizer koruması, kullanıcı tarafından kapalı
listener'lar, factory seviyesinde autoStartup=false, startup/queue/broker opt-out'ları,
ilk çıkışın start kullanması, başarısız start ownership/retry ve final factory fail-fast.

## Kubernetes doğrulaması

[Startup ham sonuçları](startup-fix-k8s/summary.md), MicroK8s v1.35.6:

| Spring Boot | Startup vakası | Kontrol | PASS | FAIL |
|---|---:|---:|---:|---:|
| 2.7.18 | 5 | 146 | 146 | 0 |
| 3.5.9 | 5 | 146 | 146 | 0 |
| 4.0.2 | 5 | 146 | 146 | 0 |
| Toplam | 15 | 438 | 438 | 0 |

Koşu exit code 0 ile tamamlandı: `completed=true`, `namespace_cleaned=true`, altyapı
hatası yok. Build JVM'i Java 17.0.12, pod JVM'i Temurin 17.0.20.1. Her üç executable
sample içindeki starter SHA-256 değeri yukarıdaki tek aday JAR'ıyla eşleşti. Pod'a yüklenen
sample JAR'ının hash'i de yereldekiyle karşılaştırıldı.

[Snapshot tüketim özeti](startup-fix-k8s/consumption-evidence.json): **15 vakanın tamamında
bakım çıkışından önce Kafka 0, Rabbit 0; çıkıştan sonra Kafka 8/8 ve Rabbit 8/8** seed
mesaj işlendi. Önceki yayımlanmış 1.3.0'ın normal flat vakasında iki broker'da da 8/8 mesaj
bakım çıkışından önce işleniyordu; tarihsel snapshot'lar orijinal raporda duruyor.

Her Boot sürümünde flat, nested, env-flat, env-nested ve cold-window vakaları ayrı pod
ile çalışır. Her pod başlamadan iki broker'a sekizer benzersiz mesaj konur. Bakımdan
çıkmadan sıfır tüketim; sonrasında sekiz mesajın tamamının ve ek pending mesajın tüketilmesi
aranır. İlk çıkış ve ikinci bakım döngüsünde kullanıcı-disabled listener'lar kapalı kalmalıdır.
Cold-window yalnızca 2 s earlier ApplicationStarted listener gecikmesi ekler; doğal
startup vakaları da ayrıca gecikmesiz çalışır.

Readiness 503, liveness 200, uygun Service backend'inin olmaması, yönetim erişiminin
sürmesi ve çıkış sonrası Service/readiness/scheduler toparlanması kontrol edilir. İkinci
döngüde Kafka running+paused, Rabbit stopped olmalı, mesaj tüketilmemeli; ikinci çıkışta
tüketim devam etmelidir. Kubernetes iş trafiğini yönlendirir; HTTP admission filter yoktur.

## Bilinen davranışlar ve kapsam dışı düzeltmeler

[Orijinal 1.3.0 raporu](V130_REVIEW.md) tarihsel kanıtlarıyla korunmuştur. Kullanıcı kararı
uyarınca erken çıkış yarışı, uppercase env/property önceliği ve startup hata raporlama
bulguları bu değişiklikte çözülmedi. [Kullanım kısıtları](../../docs/operations.md#startup-maintenance)
belgelendi. Seçilen startup matrisi bu ayrı vakaları kapsamaz; full review suite'ten
silinmediler ve PASS olarak yeniden sınıflandırılmadılar.

Pod restart kalıcılığı, trafik filtreleme veya rollout orkestrasyonu eklenmedi.
GitHub Actions akışı değiştirilmedi; benchmark testleri CI'a bağlanmadı.

## Tekrar çalıştırma

Aday kaynakları geçici bir proje kopyasında izole SNAPSHOT sürümüyle build/install edin;
yayımlanmış 1.3.0 koordinatının üzerine kurmayın. Kaynak matrisi:

```sh
python3 benchmark/test-library.py --label startup-source-repeat \
  --java-home /path/to/jdk17 --versions 3.5.9 4.0.2 --verify
```

Yerel aday kurulduktan sonra (yeni build'de hash'i yeniden hesaplayın):

```sh
python3 benchmark/kubernetes/startup.py --label startup-fix-repeat \
  --starter-version 1.3.1-startup-fix-SNAPSHOT --skip-build-starter \
  --expected-starter-sha256 YOUR_CANDIDATE_SHA256 --expect-deferred-startup \
  --cases flat nested env-flat env-nested cold-window \
  --java-home /path/to/jdk17 --versions 2.7.18 3.5.9 4.0.2
python3 benchmark/kubernetes/run.py --label startup-regression-repeat \
  --starter-version 1.3.1-startup-fix-SNAPSHOT --skip-build-starter \
  --expected-starter-sha256 YOUR_CANDIDATE_SHA256 \
  --java-home /path/to/jdk17 --versions 2.7.18 3.5.9 4.0.2
```

E2E runner'larını sırayla çalıştırın. Pod restart/replacement testi eklenmedi; UID ve
restartCount yalnızca mevcut pod üzerinde toparlanmanın kanıtı olarak kontrol edilir.

## Mevcut dinamik bakım akışının regresyonu

[Aynı aday JAR ile iki pod'lu Kubernetes regresyonu](startup-fix-regression-k8s/summary.md):

| Spring Boot | Kontrol | PASS | FAIL |
|---|---:|---:|---:|
| 2.7.18 | 35 | 35 | 0 |
| 3.5.9 | 35 | 35 | 0 |
| 4.0.2 | 35 | 35 | 0 |
| Toplam | 105 | 105 | 0 |

Startup modu kapalı regresyonda Service/EndpointSlice uygunluğu, tüm pod'lar unready
iken yeni Service trafiğinin kesilmesi, ayrı Pod-IP üzerinden yönetim erişiminin devam
etmesi, başarısız exit'te readiness reddinin korunması ve başarılı exit'te Service'e dönüş
geçti. Doğrudan Pod-IP HTTP erişimi ve mevcut bağlantıların sürmesi beklenen Kubernetes
davranışı olarak değerlendirildi. Tüm örnekler aynı aday starter hash'ini tüketti.

Her iki Kubernetes koşusu exit code 0, `completed=true`, `namespace_cleaned=true` ile
bitti. İki namespace'in artık cluster'da bulunmadığı ayrıca kubectl ile doğrulandı.
MicroK8s başlangıçta çalışıyordu ve çalışır bırakıldı. Son kontrolde aday main kaynak
hash'i çalışma ağacıyla, yayımlanmış 1.3.0 hash'i orijinaliyle eşleşti; GitHub Actions,
property çözümleme ve startup initializer kaynakları değiştirilmedi.

Sonuç: seçilen başlangıç tüketimi düzeltmesi **438/438 startup + 105/105 dinamik
regresyon kontrolüyle**, kaynak matrisi de **her Boot BOM'unda 188/188 testle** doğrulandı.
Bu sonuç bilinen erken çıkış/property/startup hata raporlama davranışlarının çözüldüğü
veya yeni bir sürümün yayımlandığı anlamına gelmez.

## 1.3.1 release paketleme kontrolü — 2026-11-07

Release hazırlığında root ve sample varsayılan sürümleri 1.3.1'e yükseltildi.
Java 17 / Boot 3.5.9 ile `clean verify` yeniden çalıştırıldı: **188/188 PASS**, sıfır
failure/error/skipped; JAR, sources ve Javadoc üretildi.
[1.3.1 paketleme kanıtı](release-1.3.1/packaging.json): release JAR'ındaki Maven sürüm/POM
metadata'sı dışındaki **39 dosya (36 class dahil)** Kubernetes'te test edilen adayla
byte-for-byte aynı. Böylece yukarıdaki Kubernetes kanıtı değişmeyen kodla ilişkilendirildi;
ayrı bir signed Central binary testi yapılmış gibi raporlanmadı. Önceki aday koşusundaki
1.3.0 sürümüne ilişkin kayıtlar tarihsel ölçüm olarak korundu.
