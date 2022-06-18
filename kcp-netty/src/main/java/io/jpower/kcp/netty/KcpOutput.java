package io.jpower.kcp.netty;

import io.netty.buffer.ByteBuf;

/**
 * 提供给上层调用，将数据发送给底层
 * @author <a href="mailto:szhnet@gmail.com">szh</a>
 */
public interface KcpOutput {

    void out(ByteBuf data, Kcp kcp);

}
