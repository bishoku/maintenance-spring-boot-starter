# 1.3.0 kod review — Kubernetes başlangıç bakım modu

Review edilen etiket: v1.3.0, commit `a00182d`. Karşılaştırma: `b979d25..v1.3.0`.
Takip: kullanıcı kararıyla erken tüketim için ayrı bir düzeltme uygulandı;
[aday artifact doğrulaması](STARTUP_CONSUMPTION_FIX.md) bu tarihsel rapordan ayrıdır.
Diğer bulgular kullanım kısıtı/bilinen davranış olarak korunuyor.

Tarih: 2026-11-06. Kütüphane kaynakları değiştirilmeden yayımlanmış Central JAR'ı kullanıldı.
SHA-256: `bc5ffa4e40b304135087540e9693586d543c123d53e32a00e4604249e90b707d`.

## Bulgular

### P1 — Broker listener'ları startup pause uygulanmadan mesaj tüketiyor

Konum: `src/main/java/com/bishokudev/maintenance/manager/MaintenanceStartupInitializer.java:107-121`.
Pause, ApplicationStartedEvent sırasında yapılıyor. SmartLifecycle consumer'ları context
refresh aşamasında zaten çalışmaya başlıyor. Bu nedenle son durumda readiness 503 ve
consumer'lar durmuş görünse bile bakımda açılan uygulama broker işini önceden işlemiş oluyor.

Kubernetes flat/nested/env başlangıçlarında her broker'a pod başlamadan sekiz benzersiz
mesaj kondu. Yönetimden çıkış çağrılmadan bu mesajlar hem Kafka hem Rabbit handler'larında
görüldü. Bounded earlier-listener senaryosu aynı doğal pencereyi büyütüyor; bulgu için
bu gecikme gerekli değil, normal flat senaryoda da mevcut.

Öneri: listener lifecycle'ını başlamadan önce kontrol edin ve startup'a ait ownership'i
explicit izleyin. Daha sonra ApplicationStartedEvent'te durdurmak, sıfır cold-start
consumption garantisi sağlamıyor. Kullanıcının autoStartup=false listener'larını çıkışta
başlatmama sözleşmesi korunmalı.

### P1 — Başlangıç pause işlemi Actuator çıkışıyla aynı geçiş kilidini paylaşmıyor

Konum: `src/main/java/com/bishokudev/maintenance/manager/MaintenanceStartupInitializer.java:119`,
`:171-176`; coordinator yalnızca kendi setMaintenanceMode çağrılarını serialize ediyor.

Başlangıç stop'u gecikirken HTTP endpoint erişilebilir. Çıkış isteği henüz startup ownership
oluşmadığı için boş resume yapıp INACTIVE dönebiliyor; başlangıç thread'i sonra consumer'ları
durduruyor. Kubernetes'te HTTP 200/INACTIVE, readiness 200 ve Service trafiği mevcutken Kafka
paused/Rabbit stopped olarak kaldı. İkinci enabled=false isteği INACTIVE no-op olduğu için
toparlamıyor. Deterministik latch testi de aynı sonucu veriyor.

Öneri: startup ve dinamik geçişleri ortak bir koordinasyon/gate altında işletin; startup
askıda iken başarılı çıkış bildirmeyin. AtomicBoolean yalnızca tekrar çağrıyı engelliyor,
startup tamamlanması ile exit'in sırasını güvence altına almıyor.

### P1 — Başlangıç stop hataları ACTIVE/hatasız durumunun arkasında gizleniyor

Konum: `src/main/java/com/bishokudev/maintenance/manager/MaintenanceStartupInitializer.java:171-181`.
QueueMaintenanceManager broker hatalarını error map olarak döndürüyor. Initializer sonucu
başarı gibi saklıyor, snapshot'a FAILED/partialFailure/queue error yazmıyor. Atomic flag de
başarıdan önce true yapılıyor. started/ready fallback tekrar denemiyor; enabled=true ACTIVE
no-op'u başarısız startup'ı yeniden denemiyor.

Yayımlanmış binary'de injected stop exception sonrasında `phase=ACTIVE`,
`details={phase=ACTIVE,startup=true}`, `isQueuesPausedOnStartup=true` gözlendi. Gerçek brokerların
yanında özel lifecycle manager hata enjeksiyonu kullanılan Kubernetes senaryosu da bunu
tekrarladı. Bu senaryoda broker ağı kesintisi simüle edilmiyor.

Öneri: startup sonuçlarını aynı TransitionReport/partialFailure değerlendirmesinden geçirin,
state'e yazın ve FAILED durumundan kontrollü retry sağlayın; readiness yayınlama hatalarını da
başarı olarak raporlamayın. Erken isWorkBlocked gating'i korunabilir.

### P2 — Manuel env fallback Spring property önceliğini bozuyor

Konum: `src/main/java/com/bishokudev/maintenance/config/MaintenanceProperties.java:104-115`.
Binder'ın daha öncelikli `maintenance.start-in-maintenance=false` değerine rağmen,
Environment içindeki ham MAINTENANCE_START_IN_MAINTENANCE=true tekrar okunup true dönüyor.
Kubernetes CLI false + env true vakasında pod beklenmedik şekilde ACTIVE/readiness 503 açıldı.
Aynı desen getStartupReason içinde de var.

Öneri: env alias'larını uygun property-source önceliğinde normalize ederek Binder üzerinden
çözün. Her kaynaktan true değerlerini OR ile toplamak açık false override'larını yok sayıyor.

## Kanıtlar ve test yorumu

- [Mevcut kaynak matrisi](v130-source-jdk17/results.json): Boot 3.5.9 ve 4.0.2 için
  Java 17 üzerinde 177/177, hata ve atlanan test yok.
- [Deterministik review reproducers](v130-review-repro/results.json): her Boot'ta üç
  kabul assertion'ı başarısız, sıfır compilation/runtime test hatası. Başarısızlıklar review
  bulgularının kanıtıdır. Bu standalone test projesi root CI'a eklenmedi.
- [Yeni startup Kubernetes sonuçları](v130-startup-k8s/summary.md): Boot 3.5.9 ve 4.0.2 için
  ayrı ayrı **145 kontrol: 124 PASS / 21 FAIL**; toplam **290: 248 PASS / 42 FAIL**.
  Koşu tamamlandı (`completed=true`), namespace temizlendi (`namespace_cleaned=true`).
  Başarısız kontroller yukarıdaki dört kusuru tekrarlıyor; altyapı/derleme hatası değil.
  Her vakanın startup/recovery JSON snapshot'ları ham kanıttır.

Mevcut StartupTestApplication broker registry'leri mock; bu testler gerçek listener'ların
context refresh sırasında tüketim yapmasını göremez. Eşzamanlı stress testleri de ortak
bir startup/exit bariyerini zorunlu kılan bu interleaving'i kapsamıyor.

## Kapsam

Readiness ile Kubernetes routing hedefi korundu. Direkt Pod-IP business HTTP 200 ve mevcut
bağlantıların sürmesi hata sayılmadı. Restart kalıcılığı testi yapılmadı. İlk tick'ten itibaren
MaintenanceState.isWorkBlocked kullanan işlerin gating'i test edildi; framework scheduled
işlerinin tamamının kütüphane tarafından durdurulduğu iddia edilmiyor.

Test fixture'ları dışında kütüphane, release ve GitHub Actions değişikliği yapılmadı.
Bu review, 1.3.0 startup özelliğinin hatasız/production-ready onayı değildir.

## Kubernetes startup matrisi

Ortam: MicroK8s v1.35.6, Java 17 ile sample derleme, `eclipse-temurin:17-jre-jammy`
pod imajı, gerçek Kafka ve RabbitMQ. Her sample içine gömülen starter'ın SHA-256 değeri
Central 1.3.0 JAR'ıyla eşleşti; pod'a yüklenen sample JAR'ı da yereldekiyle karşılaştırıldı.
Startup testi her vaka için yeni pod/benzersiz Kafka grubu kullanır. Kafka önceki vakaların
mesajlarını da okuyabilir; cold-start assertion'ları yalnızca o vakaya ait sekiz token'ı sayar.

| Vaka (her Boot için) | Kontrol | PASS | FAIL | Başarısızlığın anlamı |
|---|---:|---:|---:|---|
| flat | 20 | 18 | 2 | Kafka ve Rabbit erken tüketimi |
| nested | 20 | 18 | 2 | Kafka ve Rabbit erken tüketimi |
| env-flat | 20 | 18 | 2 | Kafka ve Rabbit erken tüketimi |
| env-nested | 20 | 18 | 2 | Kafka ve Rabbit erken tüketimi |
| cold-window | 20 | 18 | 2 | Aynı tüketim penceresi, 2 s erken event-listener gecikmesi |
| queue-failure | 21 | 17 | 4 | Erken tüketim + FAILED/partialFailure raporlanmaması |
| early-exit | 9 | 7 | 2 | Başarılı exit sonrası durmuş consumer'lar ve no-op tekrar çıkış |
| property-override | 14 | 9 | 5 | CLI false yerine env true uygulanması |
| Binary kimliği | 1 | 1 | 0 | Yayımlanmış JAR hash'i |

`flat` vakasında her iki Boot sürümünde de **o vakaya ait Kafka 8/8, Rabbit 8/8**
mesaj bakım çıkışı öncesi işlendi; tüketim anındaki `workBlocked=true`, `phase=ACTIVE`
kaydedildi. Scheduler fixture'ı ise `isWorkBlocked()` kontrolünü uyguladığı için ilk
çalışmasından itibaren işi atladı.

Normal flat/nested/env vakalarında yerleşmiş bakım durumunda readiness 503, liveness 200,
boş uygun EndpointSlice kümesi, yeni Service isteğinin ulaşmaması, yönetim erişimi ve
bakımdan çıkış sonrası readiness/Service/consumer/scheduler toparlanması geçti.
`early-exit` vakasında 8 s stop bariyeri doğal eşzamanlılığı tekrarlanabilir kılar; çıkış
startup pause tamamlanmadan HTTP 200 döndü. Tekrar çıkış da consumer'ları başlatmadı.
`queue-failure` özel lifecycle manager exception'ı enjekte eder; broker kesintisi veya
pod restart senaryosu değildir.

Tekrarlama (yayımlanmış 1.3.0 bağımlılığıyla, yeni bir label kullanarak):

```sh
python3 benchmark/kubernetes/startup.py --label v130-startup-repeat \
  --skip-build-starter \
  --expected-starter-sha256 bc5ffa4e40b304135087540e9693586d543c123d53e32a00e4604249e90b707d \
  --java-home /path/to/jdk17 --versions 3.5.9 4.0.2
```

1.3.0'da sözleşme assertion'ları FAIL olduğu için runner exit code 1 döndürür.
Düzeltmeleri doğrularken yeni/izole artifact sürümü ve ona ait hash kullanılmalı;
yayımlanmış 1.3.0 yerelde değiştirilmemeli.

Framework yaşam döngüsü kontrolü:
[Spring context finishRefresh](https://raw.githubusercontent.com/spring-projects/spring-framework/v6.2.15/spring-context/src/main/java/org/springframework/context/support/AbstractApplicationContext.java)
lifecycle başlangıcını yapar;
[Boot SpringApplication](https://raw.githubusercontent.com/spring-projects/spring-boot/v3.5.9/spring-boot-project/spring-boot/src/main/java/org/springframework/boot/SpringApplication.java)
`refreshContext` sonrasında `listeners.started` çağırır. Gerçek broker gözlemi bu sırayla uyumludur.

## Mevcut dinamik bakım akışının Kubernetes regresyonu

[Yayımlanmış 1.3.0 regresyon sonuçları](v130-k8s-regression/summary.md):

| Spring Boot | Kontrol | PASS | FAIL |
|---|---:|---:|---:|
| 2.7.18 | 35 | 35 | 0 |
| 3.5.9 | 35 | 35 | 0 |
| 4.0.2 | 35 | 35 | 0 |
| Toplam | 105 | 105 | 0 |

Startup modu kapalı bu iki pod'lu regresyon, Service/EndpointSlice üyeliği, tüm pod'lar
bakımdayken yeni Service isteğinin ulaşmaması, yönetim erişiminin sürmesi, başarısız
çıkışta readiness reddinin korunması ve başarılı çıkışta trafiğin geri gelmesini doğruladı.
Açık HTTP/keep-alive bağlantılarının sürmesi ve direkt Pod-IP erişimi beklenen Kubernetes
davranışı olarak geçti. Pod değiştirme/restart yapılmadığı UID ve restartCount ile kontrol edildi;
restart sonrası kalıcılık testi değildir. Boot 2.7.18 için burada yeni startup özelliği değil,
mevcut dinamik akış test edildi. Pod JVM'i Temurin 17.0.20.1 olarak kaydedildi.

```sh
python3 benchmark/kubernetes/run.py --label v130-regression-repeat \
  --skip-build-starter \
  --expected-starter-sha256 bc5ffa4e40b304135087540e9693586d543c123d53e32a00e4604249e90b707d \
  --java-home /path/to/jdk17 --versions 2.7.18 3.5.9 4.0.2
```

Regresyon koşusu `completed=true`, `namespace_cleaned=true`, hata olmadan exit code 0 ile
bitti. Startup koşusu exit code 1 ile tamamlandı; nedeni 42 sözleşme assertion'ının dört
bulguyu tekrar etmesi. Her iki koşunun namespace'inin cluster'dan kaldırıldığı ayrıca
kubectl ile doğrulandı. Başlangıçta çalışır durumdaki MicroK8s çalışır bırakıldı.

Kabul kararı: mevcut dinamik bakım akışı bu matriste doğrulandı. Yeni startup özelliği
üç P1 ve bir P2 bulgusu giderilip aynı testler geçmeden üretime hazır olarak onaylanamaz.
