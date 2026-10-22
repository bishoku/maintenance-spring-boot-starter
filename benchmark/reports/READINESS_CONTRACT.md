# Güncel sözleşme: Kubernetes readiness, HTTP filtreleme değil

Kullanıcı netleştirmesi doğrultusunda önceki HTTP admission/drain yorumu geri alındı.
Kütüphanenin görevi readiness probe'u başarısız hale getirerek Kubernetes'in pod'u
Service'in hazır backend'lerinden çıkarmasını sağlamaktır. Trafiği kesen taraf Kubernetes'tir.

## Geri alınan kapsam genişlemesi

- MaintenanceFilter ve MaintenanceWorkTracker kaldırıldı.
- Servlet API bağımlılığı, maintenance.http ayarları ve drain-timeout kaldırıldı.
- Coordinator artık HTTP kabulünü kapatmıyor veya aktif HTTP isteklerini sayıp beklemiyor.
- `drain-delay` yalnızca readiness değişikliği sonrası konfigüre edilen yayılım payıdır.
- Doğrudan Pod-IP/localhost iş isteğinin bakımda 200 dönmesi bir hata değildir.
- Önceki raporlardaki `direct-http-rejected`, `wait-for-inflight-http` ve
  `drain-timeout-fails-closed` kriterleri bu ürünün hedefi için geçersizdir.
  Tarihsel JSON sonuçları silinmedi; eski PASS/FAIL değerlendirmeleri yeni sözleşmeye
  uyumluluk kanıtı sayılmamalıdır. Özellikle eski baseline'daki bu FAIL'ler ürün hatası değildi.

## Korunan düzeltmeler

Boot 3.5/4 uyumluluğu, hook iptali, başarısız geçişlerin FAILED olması ve yeniden
 denenebilmesi, exit hook'larının consumer resume'dan önce bitmesi, Kafka pause acknowledgement,
Rabbit listener sahipliği ve broker hata metrikleri geçerliliğini korur. Hata halinde
readiness tekrar ACCEPTING yapılmaz; readiness yayınlama hatası oluşursa bu da raporlanır.
Bu davranış, trafiğin kütüphane içinde engellendiği anlamına gelmez.

## Yeni test sözleşmesi

Compose: readiness 503 + liveness 200 + doğrudan business HTTP 200; broker ve hook
sözleşmeleri; ayarlanan grace süresi; readiness kapatıldığında grace'in atlanması.

MicroK8s: iki uygulama pod'u ve ayrı client pod'u ile gerçek ClusterIP üzerinden
request dağılımı; Pod Ready=False; EndpointSlice ready=false; yeni bağlantıların yalnızca
hazır pod'a gitmesi; tüm pod'lar unready iken yeni Service isteklerinin başarısızlığı;
mevcut bağlantı/isteğin devam etmesi; restart olmaması ve bakım sonrası routing'in dönmesi.
Adresin EndpointSlice'tan tamamen silinmesi zorunlu değildir. Endpoint kalıp `ready=false`
olabilir. Service trafiği testlerinde port-forward kullanılmaz.

## Güncel doğrulama

- [Compose](readiness-compose-v2/summary.md): 65/65 kontrol geçti (3 build + 62 davranış kontrolü), Boot 3.5.9 ve 4.0.2.
- [Java 25 kaynak testleri](readiness-source-v2/results.json): sürüm başına 80/80.
- [Java 17 clean verify](readiness-source-jdk17/results.json): sürüm başına 80/80; hata ve atlanan test yok.
- GitHub CI dosyaları değiştirilmedi.

Bu sonuçlar yalnızca belirtilen iki Boot sürümünü kapsar; bütün 4.x patch/minor
sürümleri için test edilmiş uyumluluk iddiası değildir.


## MicroK8s ilk bulgu — düzeltme öncesi

[microk8s-readiness-v4](microk8s-readiness-v4/summary.md) iki pod/tek Kafka partition
durumunda partition atanmayan consumer için pause acknowledgement timeout üretti.
Readiness ve Service yönlendirmesi doğruydu; transition yanlışlıkla FAILED oluyordu.
Spring Kafka yalnızca atanmış partition varsa consumerPaused işaretini true yapıyor.
Düzeltme: pause isteğini koruyarak atanmış partition'ı olmayan consumer'ı bekleme dışında
tutmak; concurrent container çocuklarını ayrı ayrı kontrol etmek. Atanmış partition varsa
acknowledgement zorunlu kalır. Null/henüz bilinmeyen assignment boş assignment sayılmaz.

Kaynak: [Spring Kafka 3.3.13 doPauseConsumerIfNecessary](https://github.com/spring-projects/spring-kafka/blob/v3.3.13/spring-kafka/src/main/java/org/springframework/kafka/listener/KafkaMessageListenerContainer.java).

Concurrent container resume kontrolü de alt consumer bazında yapıldı; aggregate
isContainerPaused=false, bütün çocukların resume olduğu anlamına gelmediği için
en az bir çocuk paused kaldığında geçiş beklemeye devam ediyor. Üç regresyon testi
boş assignment, karışık concurrent assignment ve kısmi resume durumlarını kapsıyor.

İlk altyapı denemeleri:
- microk8s-readiness-v2: containerd import sudo istedi; test çalışmadı.
- microk8s-readiness-v3: broker bootstrap Service döngüsü ve kısa istemci timeout'u
  giderildi; kısmi koşu, başarı kanıtı olarak kullanılmıyor.
- microk8s-readiness-v4: tamamlandı ancak 54 kontrolden 3 Kafka entry kontrolü başarısız.
- microk8s-readiness-fixed: ilk Kafka düzeltmesiyle 54/54; nihai resume düzeltmesi sonraki koşuda.

## Nihai Kubernetes sonucu

[MicroK8s nihai koşu](microk8s-readiness-final/summary.md): **54/54 PASS**,
Boot 3.5.9 ve 4.0.2 için 27'şer kontrol (sürüm başına 2 JAR bütünlük kontrolü,
25 davranış kontrolü). Kubernetes v1.35.6, iki Java 17 uygulama pod'u,
bir client pod'u, gerçek Kafka/RabbitMQ; tek node, ClusterIP.

- Bakım pod'u readiness 503 / liveness 200; Pod Ready=False ve EndpointSlice ready=false.
- Her Service dağılım kontrolünde 40 yeni bağlantı kullanıldı; bakım pod'u dışlandı.
- Tüm pod'lar bakımdayken 3 yeni Service bağlantısı başarısız oldu; HTTP 503 zorunlu tutulmadı.
- Doğrudan Pod-IP çalışmaya devam etti. 15 saniyelik mevcut istek ve aynı keep-alive
  bağlantısındaki sonraki istek tamamlandı.
- Exit hook hatası Service'e dönüş sağlamadı; başarılı retry routing'i geri getirdi.
- Pod UID ve restart sayıları değişmedi.
- İki Boot sürümü aynı starter SHA-256 değerini tüketti:
  `be862b262ddae70e17c943d5ab357a969e67d858c7dfb03f9d6b8924c1b77713`.

EndpointSlice hazır kümesinin beklenen hale geldiğinin gözlenmesi, komut başlangıcından
itibaren 3.5.9'da 2.05–2.46 s, 4.0.2'de 1.77–2.07 s sürdü. Buna API/istemci gecikmesi,
1 saniyelik drain-delay ve broker geçişleri dahildir; kube-proxy/ingress SLA'sı değildir.
Readiness probe period=1s, failureThreshold=1. Production probe ayarları, proxy ve
bağlantı davranışı için uygun grace ayrıca ölçülmelidir.

[Nihai Java 17 verify](readiness-final-jdk17/results.json) ve
[nihai Java 25 testleri](readiness-final-jdk25/results.json):
her Boot sürümünde **83/83 PASS**, hata/atlanan test yok.

Yöntem hedeflenen Kubernetes Service yönlendirmesi için doğrulandı. Bu doğrulama
ingress, çok node, HTTP/2/gRPC veya bütün mevcut bağlantıların boşalması iddiası taşımaz.
Açık bağlantıların sürmesi ve doğrudan pod erişimi bu kütüphanenin hatası değildir.

MicroK8s koşusu tamamlandı ve oluşturduğu namespace temizlendi. Cluster çalışır bırakıldı;
diğer namespace ve addon ayarları değiştirilmedi.

[Nihai Compose E2E](readiness-compose-final/summary.md): **64/64 PASS** (2 sample build,
62 davranış kontrolü). Starter yeniden build edilmeden Kubernetes'te doğrulanan aynı
SHA-256'lı JAR tüketildi. Önceki 65 kontrolün ek bir tanesi starter build kontrolüydü.
Java 25 üzerinde gerçek Kafka/RabbitMQ ile bütün senaryolar yeniden çalıştırıldı.

## Kütüphane kabul kapsamının netleştirilmesi

Production değerlendirmesindeki restart/rollout, ingress altyapısı, güvenlik kurulumu ve
operasyonel orkestrasyon önerileri bu kütüphane için açık hata veya release engeli
değildir. Kullanıcının belirlediği ek kabul kriteri: pod uygulama Service'inin hazır
endpoint'lerinden çıktıktan sonra yönetim endpoint'ine erişim ve başarılı bakım çıkışı.

Bu kriter için benchmark güçlendirildi: ayrı client pod'undan doğrudan Pod-IP ile
GET /actuator/maintenance, POST enabled=false, bağımsız durum GET'i, readiness 200,
EndpointSlice'a dönüş ve yeni Service isteklerinin başarılı olması kontrol ediliyor.
Aynı akış tüm uygulama pod'ları bakımdayken de çalıştırılıyor.
Bu test yönetim endpoint'inin kullanılabilirliğini doğrular; standart uygulama
Service'inin unready pod'a yönetim trafiği taşıdığını iddia etmez.

[Yönetim erişimi kabul koşusu](management-access/summary.md): **68/68 PASS**,
Boot 3.5.9 ve 4.0.2 için 34'er kontrol. Önceki 54 kontrol korunarak 14 açık
yönetim erişimi/çıkış doğrulaması eklendi. Service dışındaki pod için yönetim GET'i
200 döndü; çıkış POST'u ve bağımsız durum GET'i INACTIVE sonucunu doğruladı;
readiness 200 ve Service yönlendirmesi geri geldi. Tüm podlar bakımdayken yapılan
çıkışlar da geçti. Kütüphane kaynakları ve GitHub CI bu adımda değiştirilmedi.

Bu kapsam ve test edilen sürümler için açık bir kütüphane release engeli bulunmuyor.

## 1.2.0 release paketi

[Java 17 release doğrulaması](release-1.2.0-jdk17/results.json): Boot 3.5.9 ve
4.0.2 için 83/83 test. [Sürümlenmiş paket smoke testi](release-1.2.0-smoke/summary.md):
19/19 kontrol; starter/sample build ve iki Boot sürümünde gerçek Kafka/RabbitMQ yaşam
döngüsü. Benchmark runner'ları starter sürümünü artık root POM'dan alır.
Benchmarklar GitHub Actions'a eklenmedi; release workflow yalnızca kaynak testleri,
imzalama, Central yayını ve GitHub release dosyalarını kapsar.
