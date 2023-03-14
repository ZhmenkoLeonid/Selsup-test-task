package com.zhmenko.selsup;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.TimedSemaphore;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

@Slf4j
public class CrptApi {
    private static final String URL_CREATE_DOCUMENT = "https://markirovka.crpt.ru/api/v3/lk/documents/create?pg=";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final HttpClient httpClient;
    private final TimedSemaphore timedSemaphore;
    private final ExecutorService executorService;
    private final Service.TokenService tokenService;
    // Пример использования
    public static void main(String[] args) throws InterruptedException {
        TimeUnit timeUnit = TimeUnit.SECONDS;
        int requestLimit = 1;
        // разрешаем {requestLimit} запросов в {timeUnit}
        CrptApi crptApi = new CrptApi(timeUnit, requestLimit);

        CrptApi.Model.Request.LPIntroduceGoodsDocument lpIntroduceGoodsDocument =
                CrptApi.Model.Request.LPIntroduceGoodsDocument.builder().build();

        CrptApi.Model.Request.CreateDocumentRequest createDocumentRequest =
                CrptApi.Model.Request.CreateDocumentRequest.builder()
                        .signature("sign")
                        .documentFormat(CrptApi.Model.DocumentFormat.MANUAL)
                        .productGroup(CrptApi.Model.ProductGroup.CLOTHES)
                        .type(CrptApi.Model.DocumentType.LP_INTRODUCE_GOODS)
                        .productDocument(lpIntroduceGoodsDocument)
                        .build();
        // Вызываем метод создания документа {threadNum} раз. Каждый вызов выполняется в отдельном потоке
        int threadNum = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        for (int i = 0; i < threadNum; i++) {
            executorService.submit(() -> {
                try {
                    Future<Model.Response.CreateDocumentResponse> responseFuture =
                            crptApi.createDocumentLPIntroduceGoods(createDocumentRequest);
                    System.out.println(responseFuture.get());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            });
        }
        executorService.awaitTermination(threadNum/requestLimit + 1, timeUnit);
    }

    public CrptApi() {
        this(TimeUnit.MINUTES, 5);
    }

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        if (requestLimit < 1)
            throw new IllegalArgumentException("Ограничение на количество запросов должно быть положительным числом");
        Objects.requireNonNull(timeUnit);

        timedSemaphore = new TimedSemaphore(1, timeUnit, requestLimit);
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(requestLimit);
        connManager.setDefaultMaxPerRoute(requestLimit);
        this.httpClient = HttpClients.custom().setConnectionManager(connManager).build();
        this.executorService = Executors.newFixedThreadPool(requestLimit);
        this.tokenService = new Service.Impl.TokenServiceImpl(httpClient);
    }

    private Future<Model.Response.CreateDocumentResponse> createDocument(Model.Request.CreateDocumentRequest createDocumentRequest) {
        return executorService.submit(() -> {
            try {
                timedSemaphore.acquire();

                Model.Response.TokenResponse tokenResponse = tokenService.obtainToken(createDocumentRequest.getSignature());
                if (tokenResponse.getToken() == null) {
                    log.error(tokenResponse.errorToString());
                    throw new Model.Exception.TokenException("Ошибка при получении токена!");
                }
                HttpPost request = new HttpPost(URL_CREATE_DOCUMENT + createDocumentRequest.getProductGroup().getValue());
                StringEntity requestEntity = new StringEntity(objectMapper.writeValueAsString(createDocumentRequest),
                        ContentType.APPLICATION_JSON);
                request.setEntity(requestEntity);
                request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + tokenResponse.getToken());
                request.setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8");
                HttpResponse response = httpClient.execute(request);
                return objectMapper
                        .readValue(EntityUtils.toString(response.getEntity()), Model.Response.CreateDocumentResponse.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public Future<Model.Response.CreateDocumentResponse> createDocumentLPIntroduceGoods(Model.Request.CreateDocumentRequest createDocumentRequest) {
        if (createDocumentRequest.getType() != Model.DocumentType.LP_INTRODUCE_GOODS)
            throw new IllegalArgumentException("Доступен только тип LP_INTRODUCE_GOODS для ввода в оборот товара, произведённого в РФ!");
        if (createDocumentRequest.getDocumentFormat() != Model.DocumentFormat.MANUAL)
            throw new IllegalArgumentException("Доступен только JSON формат! (DocumentFormat.MANUAL)");

        return createDocument(createDocumentRequest);
    }

    static class Model {
        static class Exception {
            static class HttpRequestException extends RuntimeException {
                public HttpRequestException(String message) {
                    super(message);
                }
            }

            static class TokenException extends RuntimeException {
                public TokenException(String message) {
                    super(message);
                }
            }
        }

        static class Request {
            abstract static class AbstractDocument {
                public final String toBase64JsonString() throws JsonProcessingException {
                    return Base64.getEncoder().encodeToString(objectMapper.writeValueAsBytes(this));
                }
            }

            @Data
            @Builder
            @JsonInclude(JsonInclude.Include.NON_NULL)
            static class CreateDocumentRequest {
                @JsonSerialize(using = AbstractDocumentSerializer.class)
                @JsonProperty("product_document")
                private AbstractDocument productDocument;
                @JsonSerialize(using = SignatureSerializer.class)
                private String signature;
                @JsonProperty("product_group")
                private ProductGroup productGroup;
                private DocumentType type;
                @JsonProperty("document_format")
                private DocumentFormat documentFormat;

                static class AbstractDocumentSerializer extends JsonSerializer<AbstractDocument> {
                    @Override
                    public void serialize(AbstractDocument abstractDocument,
                                          JsonGenerator jsonGenerator,
                                          SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeObject(abstractDocument.toBase64JsonString());
                    }
                }

                static class SignatureSerializer extends JsonSerializer<String> {

                    @Override
                    public void serialize(String signature,
                                          JsonGenerator jsonGenerator,
                                          SerializerProvider serializerProvider)
                            throws IOException {
                        jsonGenerator.writeObject(Base64.getEncoder().encodeToString(signature.getBytes()));
                    }
                }
            }

            @Data
            @Builder
            static class Description {
                private String participantInn;
            }

            @Data
            @Builder
            static class LPIntroduceGoodsDocument extends AbstractDocument {
                @JsonProperty("reg_number")
                private String regNumber;
                @JsonProperty("production_date")
                private String productionDate;
                private Description description;
                @JsonProperty("doc_type")
                private String docType;
                @JsonProperty("doc_id")
                private String docId;
                @JsonProperty("owner_inn")
                private String ownerInn;
                private List<ProductsItem> products;
                @JsonProperty("reg_date")
                private String regDate;
                @JsonProperty("participant_inn")
                private String participantInn;
                @JsonProperty("doc_status")
                private String docStatus;
                private boolean importRequest;
                @JsonProperty("production_type")
                private String productionType;
                @JsonProperty("producer_inn")
                private String producerInn;
            }

            @Data
            @Builder
            static class ProductsItem {
                @JsonProperty("uitu_code")
                private String uituCode;
                @JsonProperty("certificate_document_date")
                private String certificateDocumentDate;
                @JsonProperty("production_date")
                private String productionDate;
                @JsonProperty("certificate_document_number")
                private String certificateDocumentNumber;
                @JsonProperty("tnved_code")
                private String tnvedCode;
                @JsonProperty("certificate_document")
                private String certificateDocument;
                @JsonProperty("producer_inn")
                private String producerInn;
                @JsonProperty("owner_inn")
                private String ownerInn;
                @JsonProperty("uit_code")
                private String uitCode;
            }
        }

        static class Response {
            @Data
            @JsonIgnoreProperties(ignoreUnknown = true)
            static class CreateDocumentResponse {
                private String value;
                private String code;
                @JsonProperty("error_message")
                private String errorMessage;
                private String description;
            }

            @Data
            static class KeyMessageResponse {
                private String uuid;
                private String data;
            }

            @Data
            @JsonIgnoreProperties(ignoreUnknown = true)
            static class TokenResponse {
                private String token;
                private String code;
                @JsonProperty("error_message")
                private String errorMessage;
                private String description;


                public String errorToString() {
                    return "TokenResponseError{" +
                            "code='" + code + '\'' +
                            ", errorMessage='" + errorMessage + '\'' +
                            ", description='" + description + '\'' +
                            '}';
                }
            }
        }

        enum DocumentFormat {
            MANUAL("MANUAL");

            private final String value;

            DocumentFormat(String value) {
                this.value = value;
            }
        }

        enum DocumentType {
            LP_INTRODUCE_GOODS("LP_INTRODUCE_GOODS");

            private final String value;

            DocumentType(String value) {
                this.value = value;
            }

            public String getValue() {
                return value;
            }
        }

        enum ProductGroup {
            CLOTHES("clothes"),
            SHOES("shoes"),
            TOBACCO("tobacco"),
            PERFUMERY("perfumery"),
            TIRES("tires"),
            ELECTRONICS("electronics"),
            PHARMA("pharma"),
            MILK("milk"),
            BICYCLE("bicycle"),
            WHEELCHAIRS("wheelchairs");

            private final String value;

            ProductGroup(String value) {
                this.value = value;
            }

            public String getValue() {
                return value;
            }
        }
    }

    static class Service {
        static class Impl {
            // Для тестирования, т.к. в текущих условиях получить токен нельзя
            static class TokenServiceStubImpl implements TokenService {
                @Override
                public Model.Response.TokenResponse obtainToken(String signature) {
                    Model.Response.TokenResponse tokenResponse = new Model.Response.TokenResponse();
                    tokenResponse.setToken("<token>");
                    return tokenResponse;
                }
            }

            @Slf4j
            @RequiredArgsConstructor
            // Сервис для получения токена доступа
            static class TokenServiceImpl implements TokenService {
                private static final String URL_OBTAIN_KEY_MESSAGE = "https://ismp.crpt.ru/api/v3/auth/cert/key";
                private static final String URL_OBTAIN_TOKEN = "https://ismp.crpt.ru/api/v3/auth/cert";
                // Время жизни токена доступа в миллисекундах. По документации это 10 часов. Поставил 9 часов для подстраховки
                private static final long TOKEN_TTL_MILLIS = 1000 * 60 * 60 * 9;
                private final HttpClient httpClient;

                // Момент времени в миллисекундах, в который токен перестаёт быть валидным, и его нужно обновить
                private long tokenExpirationTimeMillis;

                private String tokenValue;

                // Получение токена доступа
                public Model.Response.TokenResponse obtainToken(String signature) {
                    log.info("Начинаю процесс получения токена доступа");
                    // Если токен был получен ранее и он валидный, то возвращаем его
                    if (isTokenValid()) {
                        log.info("Валидный токен уже имеется. Новый получать не требуется!");
                        Model.Response.TokenResponse tokenResponse = new Model.Response.TokenResponse();
                        tokenResponse.setToken(tokenValue);
                        return tokenResponse;
                    }
                    // Получаем сообщение, которое необходимо подписать и отправить для получения токена
                    Model.Response.KeyMessageResponse keyMessageResponse = obtainKeyMessage();
                    // Подписываем строку, которая пришла нам, и кодируем в Base64
                    keyMessageResponse.setData(
                            Base64.getEncoder().encodeToString(Utils.signDocument(keyMessageResponse.getData(), signature).getBytes())
                    );
                    try {
                        HttpPost request = new HttpPost(URL_OBTAIN_TOKEN);
                        StringEntity requestEntity = new StringEntity(objectMapper.writeValueAsString(keyMessageResponse),
                                ContentType.APPLICATION_JSON);
                        request.setEntity(requestEntity);
                        request.setHeader(HttpHeaders.ACCEPT, "application/json;charset=UTF-8");
                        HttpResponse response = httpClient.execute(request);
                        Model.Response.TokenResponse tokenResponse = objectMapper
                                .readValue(EntityUtils.toString(response.getEntity()), Model.Response.TokenResponse.class);

                        // Если получили токен, то запоминаем его значение и время истечения срока жизни токена
                        if (tokenResponse.getToken() != null) {
                            tokenValue = tokenResponse.getToken();
                            tokenExpirationTimeMillis = System.currentTimeMillis() + TOKEN_TTL_MILLIS;
                            log.info("Токен доступа был успешно получен!");
                        } else log.error("Сервер вернул ответ, но токена доступа в нём нет!");

                        return tokenResponse;
                    } catch (IOException e) {
                        throw new Model.Exception.HttpRequestException("Ошибка при получении токена доступа");
                    }
                }

                // Получение сообщения, необходимого для получения токена доступа
                private Model.Response.KeyMessageResponse obtainKeyMessage() {
                    log.info("Начинаю получать сообщение, используемое при получении токена доступа");
                    try {
                        HttpGet request = new HttpGet(URL_OBTAIN_KEY_MESSAGE);
                        HttpResponse response = httpClient.execute(request);
                        String responseBody = EntityUtils.toString(response.getEntity());
                        log.info("Успешное получение сообщения: " + responseBody);
                        return objectMapper.readValue(responseBody, Model.Response.KeyMessageResponse.class);
                    } catch (IOException e) {
                        throw new Model.Exception.HttpRequestException("Ошибка при получении сообщения, необходимого для получения токена доступа");
                    }
                }

                private boolean isTokenValid() {
                    // Проверяем, что токен был уже получен и что он валидный
                    return tokenValue != null && (tokenExpirationTimeMillis - System.currentTimeMillis() > 0);
                }
            }
        }

        interface TokenService {
            Model.Response.TokenResponse obtainToken(String signature);
        }

    }

    @UtilityClass
    static class Utils {
        // Не нашёл как подписывается документ, поэтому оставлю такой вариант
        public String signDocument(String document, String signature) {
            return document + signature;
        }
    }
}