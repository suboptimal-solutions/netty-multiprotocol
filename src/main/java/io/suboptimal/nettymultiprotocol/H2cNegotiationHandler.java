package io.suboptimal.nettymultiprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.function.Consumer;

import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.netty.handler.codec.http2.Http2CodecUtil.connectionPrefaceBuf;

/**
 * First handler in the plaintext pipeline. Peeks at incoming bytes to detect
 * HTTP/2 connection preface with prior knowledge, then configures the pipeline.
 */
class H2cNegotiationHandler extends ByteToMessageDecoder {

    private static final ByteBuf CONNECTION_PREFACE = unreleasableBuffer(connectionPrefaceBuf()).asReadOnly();

    private final AppProtocolRegistry registry;
    private final @Nullable Consumer<ChannelPipeline> http1Customizer;
    private final @Nullable Consumer<ChannelPipeline> http2Customizer;
    private final @Nullable Consumer<ChannelPipeline> http2StreamCustomizer;

    H2cNegotiationHandler(AppProtocolRegistry registry,
                          @Nullable Consumer<ChannelPipeline> http1Customizer,
                          @Nullable Consumer<ChannelPipeline> http2Customizer,
                          @Nullable Consumer<ChannelPipeline> http2StreamCustomizer) {
        this.registry = registry;
        this.http1Customizer = http1Customizer;
        this.http2Customizer = http2Customizer;
        this.http2StreamCustomizer = http2StreamCustomizer;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        int prefaceLength = CONNECTION_PREFACE.readableBytes();
        int bytesRead = Math.min(in.readableBytes(), prefaceLength);
        ChannelPipeline pipeline = ctx.pipeline();

        if (!ByteBufUtil.equals(CONNECTION_PREFACE, CONNECTION_PREFACE.readerIndex(),
                in, in.readerIndex(), bytesRead))
        {
            HttpPipelines.http1(pipeline, registry, true);
            if (http1Customizer != null) {
                http1Customizer.accept(pipeline);
            }
            pipeline.remove(this);
        } else if (bytesRead == prefaceLength) {
            HttpPipelines.http2(pipeline, registry, http2StreamCustomizer);
            if (http2Customizer != null) {
                http2Customizer.accept(pipeline);
            }
            pipeline.remove(this);
        }
    }
}
