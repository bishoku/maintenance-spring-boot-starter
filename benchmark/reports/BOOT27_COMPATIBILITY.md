# Spring Boot 2.7 — yayımlanmış 1.2.0 JAR doğrulaması

Tarih: 2026-10-23. Kütüphane kaynak commit'i: `51e377a87561d1b40a6039cc0e68e7cefdb0b1db`.

## Sonuç ve sözleşme

Mevcut tek **1.2.0** JAR'ı, aşağıdaki Boot/JVM kombinasyonlarında bakım, broker,
hook, failure/retry ve Kubernetes yönetimden çıkış sözleşmesini karşılıyor.
Kütüphane kaynaklarında, POM'da veya GitHub Actions'ta değişiklik gerekmiyor.
Boot 2.7 için yalnızca uygulama endpoint konfigürasyonu legacy `enabled` modelini kullanır.
Access `none/read-only/unrestricted` semantiği kullanıcı kararı doğrultusunda kapsam dışındadır.
`enabled=false` ile kapatma ayrıca doğrulandı.

Bu bir runtime uyumluluk doğrulamasıdır. Kütüphaneyi 2.7 BOM'u ile yeniden derleme
iddiası değildir; kaynak baseline'ı modern Actuator anotasyonlarını korur.
Aşağıdaki liste doğrulanmış sürümleri gösterir; her ara patch veya gelecekteki JVM
sürümlerinin tek tek test edildiği anlamına gelmez.

## Sonuç matrisi

| Spring Boot | Java | Altyapı | Sonuç | Ham sonuçlar |
|---|---|---|---|---|
| 2.7.0 | 17.0.12 | Compose | 30/30 PASS | [boot27-compose-jdk17-isolated](boot27-compose-jdk17-isolated/summary.md) |
| 2.7.18 | 17.0.12 | Compose | 30/30 PASS | [boot27-compose-jdk17-isolated](boot27-compose-jdk17-isolated/summary.md) |
| 2.7.18 | 21.0.8 | Compose | 30/30 PASS | [boot27-compose-jdk21](boot27-compose-jdk21/summary.md) |
| 2.7.0 | 17.0.20.1 (pod) | MicroK8s | 35/35 PASS | [boot27-k8s-jdk17](boot27-k8s-jdk17/summary.md) |
| 2.7.18 | 17.0.20.1 (pod) | MicroK8s | 35/35 PASS | [boot27-k8s-jdk17](boot27-k8s-jdk17/summary.md) |

Compose: toplam **90/90** (3 kombinasyon × 30). Her kombinasyonda sample build ve
binary kimliği dahil 2 kontrol, 11 senaryoda 28 davranış kontrolü var. `readonly`
senaryosunun 3 access kontrolü çalıştırılmadı; istisnalar JSON/Markdown'da açıkça listeli.

MicroK8s: toplam **70/70** (2 sürüm × 35). Her sürümde starter kimliği, iki pod'daki
sample JAR bütünlüğü ve 32 davranış kontrolü var. Koşu tamamlandı ve namespace temizlendi.

## Test edilen binary ve bağımlılıklar

JAR ve POM Maven Central'dan indirildi; starter tekrar derlenmedi. Bütün çalıştırılabilir
sample JAR'larının içindeki starter SHA-256 değeri kontrol edildi:

```
690cd742f734b4c9ed2e35afaa20e2eccdde70a4fcdbb4679ac1617c071b3eca
```

Kaynak: `https://repo.maven.apache.org/maven2/com/bishokudev/maintenance-spring-boot-starter/1.2.0/`.
Uygulamanın Boot BOM'u, starter'ın modern build BOM'u yerine aşağıdaki runtime
bağımlılıklarını yönetti. Tam liste her `results.json` dosyasındaki `runtime_libraries` alanında.

| Boot | Spring Framework | Spring Kafka | Spring Rabbit | Micrometer |
|---|---|---|---|---|
| 2.7.0 | 5.3.20 | 2.8.6 | 2.4.5 | 1.9.0 |
| 2.7.18 | 5.3.31 | 2.8.11 | 2.4.17 | 1.9.17 |

## Compose kapsamı

- Kafka pause acknowledgement, çalışır durumda kalma ve çıkışta resume.
- Rabbit listener stop, çıkışta restart, önceden durmuş listener'ların korunması.
- Bakımda mesaj tüketiminin durması; sıradaki mesajların çıkışta tüketilmesi.
- In-flight broker handler'larının bitmesi ve hook'ların consumer resume'dan önce çalışması.
- Opsiyonel reason; giriş/çıkış hook hataları, FAILED durumu ve başarılı retry.
- Hook timeout/interrupt; broker hata bilgisinin ve failure metriğinin korunması.
- Readiness 503 / liveness 200; doğrudan business HTTP erişiminin sürmesi.
- Konfigüre edilen propagation grace, devam eden HTTP isteğinin kesilmemesi.
- Eşzamanlı girişin idempotent olması; legacy endpoint kapatma.
- Readiness özelliği kapalıyken probe'un açık kalması ve grace'in atlanması.

## Kubernetes kapsamı

Tek node MicroK8s v1.35.6, iki uygulama pod'u, ayrı client pod'u, gerçek ClusterIP ve
EndpointSlice; Kafka 3.9.1, RabbitMQ 4.1.0. JRE sürümü her Boot sürümü için `pod_java`
alanında kayıtlı; local sample build Java 17.0.12 ile yapıldı.

İki pod, sürüm başına aynı Kafka group ve tek partition'ı kullanır. Böylece partition
atanmayan consumer'ın bakım geçişi de korunur. Sürümler arası group ayrıdır.

Pod Service'in hazır endpoint kümesinden çıktıktan sonra doğrudan Pod-IP üzerinden
GET /actuator/maintenance ve POST enabled=false başarılı oldu. POST cevabının ardından
bağımsız GET ile INACTIVE, readiness ile 200, EndpointSlice ve yeni Service bağlantılarıyla
routing'in geri gelmesi doğrulandı. Tüm podlar unready iken de yönetimden çıkış çalıştı.
Exit hook hatası pod'u erken Service'e döndürmedi; retry toparladı.

Mevcut istek ve keep-alive bağlantısı sürdü; test penceresinde pod UID/restart sayıları
korundu. Bu, restart sonrası bakım kalıcılığı veya dış yönetim ağı testi değildir.

## İlk koşudaki test izolasyonu bulgusu

[boot27-compose-jdk17](boot27-compose-jdk17/summary.md) tamamlanmadan durduruldu.
İlk lifecycle geçti; sonraki üç senaryoda henüz senaryo çalışmadan broker warm-up
beklemesi aşıldı. Aynı group'un taze JVM'ler arasında yeniden kullanılması, önceki
üyelerin rebalance/session süresini sonraki senaryoya taşıyordu.

Fixture, her Compose senaryosuna ayrı Kafka group verecek şekilde düzeltildi. Aynı
kütüphane binary'siyle Java 17 tekrar koşusu ve Java 21 koşusu tamamen geçti.
İlk FAIL'ler silinmedi ve sonuç toplamlarına katılmadı; ürün davranış testi olarak
raporlanmıyor. MicroK8s'te iki pod'un aynı group'u paylaşması özellikle korunuyor.

## Tekrarlama

[Benchmark rehberi](../README.md#spring-boot-27-runtime-compatibility) legacy/modern
profillerini, `--skip-build-starter`, `--expected-starter-sha256`, `--java-home`
seçeneklerini açıklar. Java 17 Compose koşusunda `--versions 2.7.0 2.7.18`,
Java 21 koşusunda `--versions 2.7.18` kullanıldı. Kubernetes koşusu:

```sh
python3 benchmark/kubernetes/run.py --label boot27-k8s-local \
  --versions 2.7.0 2.7.18 --java-home /path/to/jdk17 --skip-build-starter \
  --expected-starter-sha256 690cd742f734b4c9ed2e35afaa20e2eccdde70a4fcdbb4679ac1617c071b3eca
```

MicroK8s koşusu yalnızca kendi namespace'ini sildi; başlangıçta çalışır olan cluster
çalışır bırakıldı. Compose benchmark brokerları ve test verileri doğrulamalar sonunda temizlendi.

## Modern profil regresyonu

Yeni `modern`/`boot2` sample profil ayrımı ayrıca yayımlanmış aynı starter JAR
ve Java 17 ile Boot 3.5.9 ve 4.0.2 üzerinde kontrol edildi:
[28/28 PASS](boot27-modern-profile-regression/summary.md). Yaşam döngüsü,
read-only GET/POST kısıtları ve access=none ile endpoint kapatma geçti.
Kütüphane, POM ve CI dosyaları bu doğrulamada değişmedi.
