package org.ptm.kvservice.transpost;

import org.ptm.kvservice.db.FastDBIml;
import org.ptm.kvservice.db.models.KeyValuesObj;
import org.ptm.kvservice.transpost.netty.ExceptionHandler;
import org.ptm.kvservice.utils.BiFunctionWithException;
import org.ptm.kvservice.utils.Stats;
import com.google.common.util.concurrent.AbstractService;
import com.google.gson.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;


@SuppressWarnings("ALL")
public class HttpService extends AbstractService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpService.class);

    private static ByteBuf ok() {
        final ByteBuf OK;
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("acknowledged", new JsonPrimitive(true));
        OK = Unpooled.buffer();
        OK.writeCharSequence(jsonObject.toString(), Charset.defaultCharset());
        return OK;
    }

    private static final Gson GSON = new Gson();
    private static final JsonParser JSON_PARSER = new JsonParser();

    private ChannelFuture httpChannel = null;
    private final FastDBIml fastDB;
    private final Map<String, Map<String, BiFunctionWithException<ChannelHandlerContext, FullHttpRequest,
                Map.Entry<Long, Long>>>>
            handlers;
    private final int port;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final boolean isMaster;
    private final FastDBIml.Options options;

    public HttpService(FastDBIml fastDB, int port,
                       boolean isMaster, FastDBIml.Options options) {
        this.options = options;
        this.fastDB = fastDB;
        this.port = port;
        this.isMaster = isMaster;
        this.handlers = new HashMap<>();
        handlers.put("/api/list/insert", Map.of(HttpMethod.POST.name(), this::listInsertHandle,
                HttpMethod.GET.name(), this::listInsertHandle));
        handlers.put("/api/list/headpeek", Map.of(HttpMethod.POST.name(), this::listHeadPeekHandle,
                HttpMethod.GET.name(), this::listHeadPeekHandle));
        handlers.put("/api/list/tailpeek", Map.of(HttpMethod.POST.name(), this::listTailPeekHandle,
                HttpMethod.GET.name(), this::listTailPeekHandle));
        handlers.put("/api/list/delete", Map.of(HttpMethod.DELETE.name(), this::listDelete));
        handlers.put("/api/nkeys", Map.of(HttpMethod.POST.name(), this::countNumberOfKeysHandle,
                HttpMethod.GET.name(), this::countNumberOfKeysHandle));
        handlers.put("/api/keys", Map.of(HttpMethod.POST.name(), this::getKeysByPattern,
                HttpMethod.GET.name(), this::getKeysByPattern));
        handlers.put("/admin/clear", Map.of(HttpMethod.POST.name(), this::clear));
        handlers.put("/metrics", Map.of(HttpMethod.GET.name(), this::metrics, HttpMethod.POST.name(), this::metrics));
    }

    public Set<String> getRequestName() {
        return this.handlers.keySet();
    }

    private void runServer() throws Exception {
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
        // A helper class that simplifies server configuration
        ServerBootstrap httpBootstrap = new ServerBootstrap();

        // Configure the server
        httpBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel) throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));
                        pipeline.addLast(new ChunkedWriteHandler());
                        pipeline.addLast(new SimpleChannelInboundHandler<FullHttpRequest>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
                                try {
                                    QueryStringDecoder decoder = new QueryStringDecoder(msg.uri(), Charset.defaultCharset());
                                    if (handlers.containsKey(decoder.path()) && handlers.get(decoder.path())
                                    .containsKey(msg.method().name())) {
                                        try {
                                           Map.Entry<Long, Long> entry = handlers.get(decoder.path()).get(msg.method().name()).apply(ctx, msg);
                                           Stats.getINSTANCE().record(decoder.path(), entry.getKey(), entry.getValue());
                                        } catch (Exception e) {
                                            throw e;
                                        }
                                    } else {
                                        throw new IllegalArgumentException(String.format("Method %s and uri %s is not supported", msg.method(),
                                                msg.uri()));
                                    }
                                } catch (IllegalArgumentException e) {
                                    e.printStackTrace();
                                    ByteBuf content = buildErrorContent("Invalid parameters: " + e.getMessage());
                                    sendResponseWithData(ctx, content);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    LOGGER.error("Error when process request {} ", msg.uri());
                                    ByteBuf content = buildErrorContent(String.format("Error occurs when process request " +
                                            "with uri %s", msg.uri()));
                                    sendErrWithData(ctx, content);
                                }
                            }
                        })
                        .addLast(new ExceptionHandler());
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        this.httpChannel = httpBootstrap.bind(this.port).sync();
    }

    private Map.Entry<Long, Long> metrics(ChannelHandlerContext ctx, FullHttpRequest msg) throws IOException {
        long startTime = System.currentTimeMillis();
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(msg.uri());
        Set<String> includedParam = queryStringDecoder.parameters().containsKey("name[]") ? new HashSet<>(queryStringDecoder.parameters().get("name[]"))
                : Collections.EMPTY_SET;
        String str = Stats.getINSTANCE().scrape(includedParam);


        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeCharSequence(str, Charset.defaultCharset());
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, byteBuf.readableBytes());
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_HTML);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        ctx.write(response);
        ctx.flush();
        return new AbstractMap.SimpleEntry<>(1L, System.currentTimeMillis() - startTime);
    }

    private Map.Entry<Long, Long> clear(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        long startTime = System.currentTimeMillis();
        QueryStringDecoder queryString = new QueryStringDecoder(msg.uri());
        if (!queryString.parameters().containsKey("token") || queryString.parameters().get("token").size() == 0) {
            throw new IllegalArgumentException("token must be set");
        }
        String token = queryString.parameters().get("token").get(0);
        if (!token.equals("CLEAR")) {
            throw new IllegalArgumentException("token not match");

        }
        synchronized (this) {
            fastDB.close();
            FileUtils.cleanDirectory(new File(options.getDirName()));
            fastDB.reopen();
        }
        sendResponseWithData(ctx, ok());
        return new AbstractMap.SimpleEntry<>(1L, System.currentTimeMillis() - startTime);
    }

    private Map.Entry<Long, Long> listHeadPeekHandle(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        long startTime = System.currentTimeMillis();
        List<String> keys = new ArrayList<>();
        int n;
        try {
            String contentStr = msg.content().toString(Charset.defaultCharset());
            JsonObject jsonObject = new JsonParser().parse(contentStr).getAsJsonObject();
            JsonArray jsonArray = jsonObject
                    .get("keys").getAsJsonArray();
            n = jsonObject.get("size").getAsInt();
            for (JsonElement jsonElement : jsonArray) {
                keys.add(jsonElement.getAsString());
            }
        } catch (Exception e) {
            LOGGER.error("Error when process: {} ", msg.content().toString());
            throw new IllegalArgumentException("Request body is invalid", e);
        }

        Map.Entry<Long, Long> entry = peekProcessing(ctx, keys, n, new BiFunctionWithException<String, Integer, List<JsonPrimitive>>() {
            @Override
            public List<JsonPrimitive> apply(String key, Integer size) throws Exception {
                return fastDB.listHeadPeek(key, size);
            }
        });
        return entry;
    }

    private Map.Entry<Long, Long> listTailPeekHandle(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        List<String> keys = new ArrayList<>();
        int n;
        try {
            String contentStr = msg.content().toString(Charset.defaultCharset());
            JsonObject jsonObject = new JsonParser().parse(contentStr).getAsJsonObject();
            JsonArray jsonArray = jsonObject
                    .get("keys").getAsJsonArray();
            n = jsonObject.get("size").getAsInt();
            for (JsonElement jsonElement : jsonArray) {
                keys.add(jsonElement.getAsString());
            }
        } catch (Exception e) {
            LOGGER.error("Error when process: {} ", msg.content().toString());
            throw new IllegalArgumentException("Request body is invalid", e);
        }
        return peekProcessing(ctx, keys, n, new BiFunctionWithException<String, Integer, List<JsonPrimitive>>() {
            @Override
            public List<JsonPrimitive> apply(String key, Integer size) throws Exception {
                return fastDB.listTailPeek(key, size);
            }
        });
    }

    private Map.Entry<Long, Long> listInsertHandle(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        long startTime = System.currentTimeMillis();
        String contentStr = msg.content().toString(CharsetUtil.UTF_8);
        KeyValuesObj keyValuesObj = GSON.fromJson(contentStr, KeyValuesObj.class);
        fastDB.listInsert(keyValuesObj);
        sendResponseWithData(ctx, ok());
        return new AbstractMap.SimpleEntry<>((long)keyValuesObj.getValues().size(), System.currentTimeMillis() - startTime);

    }

    private Map.Entry<Long, Long> listDelete(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        long startTime = System.currentTimeMillis();
        QueryStringDecoder queryString = new QueryStringDecoder(msg.uri());
        if (!queryString.parameters().containsKey("key") || queryString.parameters().get("key").size() == 0) {
            throw new IllegalArgumentException("key parameter must be set");
        }
        String key = queryString.parameters().get("key").get(0);
        fastDB.listDelete(key);
        sendResponseWithData(ctx, ok());
        return new AbstractMap.SimpleEntry<>(1L, System.currentTimeMillis() - startTime);
    }

    private Map.Entry<Long, Long> countNumberOfKeysHandle(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        long startTime = System.currentTimeMillis();
        QueryStringDecoder queryString = new QueryStringDecoder(msg.uri());
        String wildCard;
        if (!queryString.parameters().containsKey("wildcard")
                || queryString.parameters().get("wildcard").size() == 0) {
            wildCard = "*";
        }
        else {
            wildCard = queryString.parameters().get("wildcard").get(0);
        }
        int numberOfKeys = fastDB.numberOfKeys(wildCard);
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("number_of_keys", new JsonPrimitive(numberOfKeys));
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeCharSequence(jsonObject.toString() + "\n", Charset.defaultCharset());
        sendResponseWithData(ctx, byteBuf);
        return new AbstractMap.SimpleEntry<>(1L, System.currentTimeMillis() - startTime);
    }

    private Map.Entry<Long, Long> getKeysByPattern(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        long startTime = System.currentTimeMillis();
        QueryStringDecoder queryString = new QueryStringDecoder(msg.uri());
        String wildCard;
        if (!queryString.parameters().containsKey("wildcard")
                || queryString.parameters().get("wildcard").size() == 0) {
            wildCard = "*";
        }
        else {
            wildCard = queryString.parameters().get("wildcard").get(0);
        }
        JsonArray jsonArray = new JsonArray();
        for (String key : fastDB.getKeys(wildCard)) {
            jsonArray.add(key);
        }

        JsonObject jsonObject = new JsonObject();
        jsonObject.add("keys", jsonArray);
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeCharSequence(jsonObject.toString() + "\n", Charset.defaultCharset());
        sendResponseWithData(ctx, byteBuf);
        return new AbstractMap.SimpleEntry<>(1L, System.currentTimeMillis() - startTime);
    }

    private static ByteBuf buildErrorContent(Object error) {
        ByteBuf content = Unpooled.buffer();
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("Error", new JsonPrimitive(error.toString()));
        content.writeCharSequence(jsonObject.toString(), Charset.defaultCharset());
        return content;
    }

    private static void sendResponseWithData(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, byteBuf);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, byteBuf.readableBytes());
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        ctx.write(response);
        ctx.flush();
    }

    private static void sendErrWithData(ChannelHandlerContext ctx, ByteBuf byteBuf) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, byteBuf);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, byteBuf.readableBytes());
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        ctx.write(response);
        ctx.flush();
    }

    private Map.Entry<Long, Long> peekProcessing(ChannelHandlerContext ctx, List<String> keys, int nElements,
                                BiFunctionWithException<String, Integer, List<JsonPrimitive>> peekOperator) {
        Collections.sort(keys);
        long startTime = System.currentTimeMillis();
        long totalOp = 0;
        ByteBuf content = Unpooled.buffer();
        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        ctx.write(response);
        for (String key : keys) {
            try {
                List<JsonPrimitive> primitives = peekOperator.apply(key, nElements);
                JsonObject jsonObject = new JsonObject();
                if (primitives != null) {
                    JsonArray jsonArray = new JsonArray();
                    primitives.forEach(primitive -> {
                        jsonArray.add(primitive);
                    });
                    jsonObject.add(key, jsonArray);
                    totalOp += jsonArray.size();
                } else {
                    jsonObject.addProperty(key, "null");
                }
                ByteBuf byteBuf = Unpooled.buffer();
                byteBuf.writeCharSequence(jsonObject.toString() + "\n", Charset.defaultCharset());
                ctx.writeAndFlush(byteBuf);
            } catch (Exception e) {
                ByteBuf byteBuf = Unpooled.buffer();
                byteBuf.writeCharSequence(String.format("Error when handle get with key %s with " +
                        "error %s", key, e.getMessage()), Charset.defaultCharset());
                ctx.writeAndFlush(byteBuf);
            }
        }
        long time = System.currentTimeMillis() - startTime;
        ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        return new AbstractMap.SimpleEntry<>((long)totalOp, System.currentTimeMillis() - startTime);

    }

    @Override
    protected void doStart() {
        try {
            runServer();
            notifyStarted();
        } catch (Exception e) {
            LOGGER.error("Error when run Http service at port {} ", port, e);
            notifyFailed(e);
        }
    }

    @Override
    protected void doStop() {
        try {
            httpChannel.channel().close();
            notifyStopped();
        } finally {
            this.bossGroup.shutdownGracefully();
            this.workerGroup.shutdownGracefully();
        }
    }
}
