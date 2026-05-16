package io.suboptimal.nettymultiprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.netty.handler.codec.http2.Http2CodecUtil.connectionPrefaceBuf;

/**
 * First handler in the plaintext pipeline. Peeks at incoming bytes to detect
 * HTTP/2 connection preface with prior knowledge, then configures the pipeline.
 */
class H2cNegotiationHandler extends ByteToMessageDecoder {

    private static final ByteBuf CONNECTION_PREFACE = unreleasableBuffer(connectionPrefaceBuf()).asReadOnly();

    private final HttpPipelineConfigurer configurer;

    H2cNegotiationHandler(HttpPipelineConfigurer configurer) {
        this.configurer = configurer;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        int prefaceLength = CONNECTION_PREFACE.readableBytes();
        int bytesRead = Math.min(in.readableBytes(), prefaceLength);
        ChannelPipeline pipeline = ctx.pipeline();

        if (!ByteBufUtil.equals(CONNECTION_PREFACE, CONNECTION_PREFACE.readerIndex(),
                in, in.readerIndex(), bytesRead)) {
            configurer.installHttp1(pipeline, true);
            pipeline.remove(this);
        } else if (bytesRead == prefaceLength) {
            configurer.installHttp2(pipeline);
            pipeline.remove(this);
        }
    }
}
