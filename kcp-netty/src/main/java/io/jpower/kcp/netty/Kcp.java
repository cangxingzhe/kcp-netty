package io.jpower.kcp.netty;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import io.jpower.kcp.netty.internal.ReItrLinkedList;
import io.jpower.kcp.netty.internal.ReusableListIterator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.internal.ObjectPool;
import io.netty.util.internal.ObjectPool.Handle;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import static io.jpower.kcp.netty.InputStateEnum.MISMATCH_CMD;
import static io.jpower.kcp.netty.InputStateEnum.NORMAL;

/**
 * Java implementation of <a href="https://github.com/skywind3000/kcp">KCP</a>
 *
 * @author <a href="mailto:szhnet@gmail.com">szh</a>
 */
public class Kcp {

    private static final InternalLogger log = InternalLoggerFactory.getInstance(Kcp.class);

    /**
     * no delay min rto
     */
    public static final int IKCP_RTO_NDL = 30;

    /**
     * normal min rto
     */
    public static final int IKCP_RTO_MIN = 100;

    public static final int IKCP_RTO_DEF = 200;

    public static final int IKCP_RTO_MAX = 60000;

    /**
     * cmd: push data
     * 数据包
     */
    public static final byte IKCP_CMD_PUSH = 81;

    /**
     * cmd: ack
     * 确认命令
     */
    public static final byte IKCP_CMD_ACK = 82;

    /**
     * cmd: window probe (ask)
     * 询问远端窗口大小
     */
    public static final byte IKCP_CMD_WASK = 83;

    /**
     * cmd: window size (tell)
     * 告诉远端自己的窗口大小
     */
    public static final byte IKCP_CMD_WINS = 84;

    /**
     * need to send IKCP_CMD_WASK
     */
    public static final int IKCP_ASK_SEND = 1;

    /**
     * need to send IKCP_CMD_WINS
     */
    public static final int IKCP_ASK_TELL = 2;

    public static final int IKCP_WND_SND = 32;

    /**
     * must >= max fragment size
     */
    public static final int IKCP_WND_RCV = 128;

    public static final int IKCP_MTU_DEF = 1400;

    public static final int IKCP_ACK_FAST = 3;

    public static final int IKCP_INTERVAL = 100;

    public static final int IKCP_OVERHEAD = 24;

    public static final int IKCP_DEADLINK = 20;

    public static final int IKCP_THRESH_INIT = 2;

    public static final int IKCP_THRESH_MIN = 2;

    /**
     * 7 secs to probe window size
     */
    public static final int IKCP_PROBE_INIT = 7000;

    /**
     * up to 120 secs to probe window
     */
    public static final int IKCP_PROBE_LIMIT = 120000;

    /**
     * max times to trigger fastack
     */
    public static final int IKCP_FASTACK_LIMIT = 5;

    private int conv;

    private int mtu = IKCP_MTU_DEF;

    private int mss = this.mtu - IKCP_OVERHEAD;

    private int state;

    /**
     * 当前未收到确认回传的 发送出去的包的 最小编号。也就是此编号前的包都已经收到确认回传了。
     */
    private long sndUna;

    /**
     * 一个要发送出去的包编号
     */
    private long sndNxt;

    /**
     * 下一个要接收的数据包的编号。也就是说此序号之前的包都已经按顺序全部收到了
     * ，下面期望收到这个序号的包（已保证数据包的连续性、顺序性）
     */
    private long rcvNxt;

    private int tsRecent;

    private int tsLastack;

    /**
     * 慢启动阈值（Slow Start Threshold，ssthresh）
     */
    private int ssthresh = IKCP_THRESH_INIT;


    private int rxRttvar;

    private int rxSrtt;

    private int rxRto = IKCP_RTO_DEF;

    private int rxMinrto = IKCP_RTO_MIN;

    /**
     * 发送窗口大小
     */
    private int sndWnd = IKCP_WND_SND;

    /**
     * 接收窗口的大小
     */
    private int rcvWnd = IKCP_WND_RCV;
    /**
     * 远端的接收窗口大小
     */
    private int rmtWnd = IKCP_WND_RCV;

    private int cwnd;

    private int probe;

    /**
     * 当前时间戳
     */
    private int current;

    /**
     * 刷新间隔
     */
    private int interval = IKCP_INTERVAL;

    private int tsFlush = IKCP_INTERVAL;

    private int xmit;

    private int maxSegXmit;

    /**
     * 开启快速重传模式，关闭流量控制
     */
    private boolean nodelay;

    /**
     * 是否第一次 update
     */
    private boolean updated;
    /**
     * 下次窗口探测时间
     */
    private int tsProbe;
    /**
     * 探测时间间隔
     */
    private int probeWait;

    /**
     * 被视为断开连接的再传输的最大数量
     */
    private int deadLink = IKCP_DEADLINK;

    /**
     * 可能发送的数据的最大数量
     */
    private int incr;

    /**
     * 待发送队列
     * 应用层的数据（在调用KCP.Send后）会进入此队列中，KCP在flush的时候根据发送窗口的大小，
     * 再决定将多少个Segment放入到snd_buf中进行发送
     */
    private LinkedList<Segment> sndQueue = new LinkedList<>();

    /**
     * 接收队列
     */
    private ReItrLinkedList<Segment> rcvQueue = new ReItrLinkedList<>();

    /**
     * 发送队列中包含两种类型的数据，已发送但是尚未被接收方确认的数据，没被发送过的数据。
     * 没发送过的数据比较好处理，直接发送即可。重点在于已经发送了但是还没被接收方确认的数
     * 据，该部分的策略直接决定着协议快速、高效与否。KCP主要使用两种策略来决定是否需要重
     * 传KCP数据包，超时重传、快速重传、选择重传。
     *
     * 送缓存池。发送出去的数据将会呆在这个池子中，等待远端的回传确认，等收到远端确认此包
     * 收到后再从snd_buf移出去。 KCP在每次flush的时候都会检查这个缓存池中的每个Segment，
     * 如果超时或者判定丢包就会重发。
     */
    private ReItrLinkedList<Segment> sndBuf = new ReItrLinkedList<>();

    /**
     * 接收到的数据会先存放到rcv_buf中。 因为数据可能是乱序到达本地的，
     * 所以接受到的数据会按sn顺序依次放入到对应的位置中。 当sn从低到高
     * 连续的数据包都收到了，则将这批连续的数据包转移到rcv_queue中。这
     * 样就保证了数据包的顺序性。
     */
    private ReItrLinkedList<Segment> rcvBuf = new ReItrLinkedList<>();

    private ReusableListIterator<Segment> rcvQueueItr = rcvQueue.listIterator();

    /**
     * 发送队列，待回复
     */
    private ReusableListIterator<Segment> sndBufItr = sndBuf.listIterator();

    private ReusableListIterator<Segment> rcvBufItr = rcvBuf.listIterator();

    /**
     * 收到包后要发送的回传确认。 在收到包时先将要回传ack的sn放入此队列中，
     * 在flush函数中再发出去。 acklist中，一个ack以(sn,timestampe)为一
     * 组的方式存储。即 [{sn1,ts1},{sn2,ts2} … ] 即 [sn1,ts1,sn2,ts2 … ]
     */
    private int[] acklist = new int[8];

    private int ackcount;

    private Object user;

    /**
     * 快速重启的阈值，大于0表示开启快速重传
     */
    private int fastresend;

    private int fastlimit = IKCP_FASTACK_LIMIT;

    private boolean nocwnd;

    private boolean stream;

    private KcpOutput output;

    private ByteBufAllocator byteBufAllocator = ByteBufAllocator.DEFAULT;

    /**
     * automatically set conv
     */
    private boolean autoSetConv;

    private KcpMetric metric = new KcpMetric(this);

    private static long int2Uint(int i) {
        return i & 0xFFFFFFFFL;
    }

    private static int ibound(int lower, int middle, int upper) {
        return Math.min(Math.max(lower, middle), upper);
    }

    private static int itimediff(int later, int earlier) {
        return later - earlier;
    }

    private static int itimediff(long later, long earlier) {
        return (int) (later - earlier);
    }

    private static void output(ByteBuf data, Kcp kcp) {
        if (log.isDebugEnabled()) {
            log.debug("{} [RO] {} bytes", kcp, data.readableBytes());
        }
        if (data.readableBytes() == 0) {
            return;
        }
        kcp.output.out(data, kcp);
    }

    private static int encodeSeg(ByteBuf buf, Segment seg) {
        int offset = buf.writerIndex();

        buf.writeIntLE(seg.conv);
        buf.writeByte(seg.cmd);
        buf.writeByte(seg.frg);
        buf.writeShortLE(seg.wnd);
        buf.writeIntLE(seg.ts);
        buf.writeIntLE((int) seg.sn);
        buf.writeIntLE((int) seg.una);
        buf.writeIntLE(seg.data.readableBytes());

        return buf.writerIndex() - offset;
    }

    /**
     * 协议结构
     *     0                         4      5     6      8 (BYTE)
     *
     *     +-------------------+----+----+----+
     *
     *     |      conv      | cmd | frg |  wnd |
     *
     *     +-------------------+----+----+----+   8
     *
     *     |         ts         |             sn           |
     *
     *     +-------------------+----------------+  16
     *
     *     |       una       |             len          |
     *
     *     +-------------------+----------------+   24
     *
     *     |                                                   |
     *
     *     |            DATA (optional)          |
     *
     *     |                                                   |
     *
     *     +-------------------------------------+
     *
     *
     *
     * KCP中会有四种数据包类型，分别是
     *
     * 1.数据包（IKCP_CMD_PUSH）：发送数据给远端
     * 2.ACK包（IKCP_CMD_ACK）：告诉远端自己收到了哪个编号的数据
     * 3.窗口大小探测包（IKCP_CMD_WASK）：询问远端的数据接收窗口还剩余多少
     * 4.窗口大小回应包（IKCP_CMD_WINS）：回应远端自己的数据接收窗口大小
     *
     */

    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.PACKAGE)
    static class Segment {

        private final Handle<Segment> recyclerHandle;

        /**
         * 2 byte unsign
         * 连接号。UDP是无连接的，conv用于表示来自于哪个客户端。对连接的一种替代
         */
        private int conv;

        /**
         * 命令字 1 byte
         */
        private byte cmd;

        /**
         * 分片序号
         * 分片，用户数据可能会被分成多个KCP包，发送出去
         *
         * frg是fragment的缩小，是一个Segment在一次Send的data中的倒序序号。
         * 在让KCP发送数据时，KCP会加入snd_queue的Segment分配序号，标记
         * Segment是这次发送数据中的倒数第几个Segment。 数据在发送出去时，由
         * 于mss的限制，数据可能被分成若干个Segment发送出去。在分segment的过
         * 程中，相应的序号就会被记录到frg中。 接收端在接收到这些segment时，就
         * 会根据frg将若干个segment合并成一个，再返回给应用层。
         */
        private short frg;

        /**
         * wnd是window的缩写； 滑动窗口大小，用于流控（Flow Control）
         * 1.当Segment做为发送数据时，此wnd为本机滑动窗口大小，用于告诉远端自己窗口剩余多少
         * 2.当Segment做为接收到数据时，此wnd为远端滑动窗口大小，本机知道了远端窗口剩余多少
         * 后，可以控制自己接下来发送数据的大小
         */
        private int wnd;

        /**
         * 即timestamp , 当前Segment发送时的时间戳
         */
        private int ts;

        /**
         * 即Sequence Number,Segment的编号
         */
        private long sn;

        /**
         *
         * una即unacknowledged,表示此编号前的所有包都已收到了。
         */
        private long una;

        // ---------------------- 流控

        /**
         * resend timestamp , 指定重发的时间戳，当当前时间超过这个时间时，则再重发一次这个包。
         */
        private int resendts;
        /**
         * rto即Retransmission TimeOut，即超时重传时间，在发送出去时根据之前的网络情况进行设置
         */
        private int rto;
        /**
         * 用于以数据驱动的快速重传机制
         */
        private int fastack;
        /**
         * 基本类似于Segment发送的次数，每发送一次会自加一。用于统计该Segment被重传了几次，用于参考，进行调节
         */
        private int xmit;





        private ByteBuf data;

        private static final ObjectPool<Segment> RECYCLER = ObjectPool.newPool(Segment::new);

        private Segment(Handle<Segment> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        void recycle(boolean releaseBuf) {
            conv = 0;
            cmd = 0;
            frg = 0;
            wnd = 0;
            ts = 0;
            sn = 0;
            una = 0;
            resendts = 0;
            rto = 0;
            fastack = 0;
            xmit = 0;
            if (releaseBuf) {
                data.release();
            }
            data = null;

            recyclerHandle.recycle(this);
        }

        static Segment createSegment(ByteBufAllocator byteBufAllocator, int size) {
            Segment seg = RECYCLER.get();
            if (size == 0) {
                seg.data = byteBufAllocator.ioBuffer(0, 0);
            } else {
                seg.data = byteBufAllocator.ioBuffer(size);
            }
            return seg;
        }

        static Segment createSegment(ByteBuf buf) {
            Segment seg = RECYCLER.get();
            seg.data = buf;
            return seg;
        }

    }

    public Kcp(int conv, KcpOutput output) {
        this.conv = conv;
        this.output = output;
    }

    public void release() {
        release(sndBuf);
        release(rcvBuf);
        release(sndQueue);
        release(rcvQueue);
    }

    private void release(List<Segment> segQueue) {
        for (Segment seg : segQueue) {
            seg.recycle(true);
        }
        segQueue.clear();
    }

    private ByteBuf tryCreateOrOutput(ByteBuf buffer, int need) {
        if (buffer == null) {
            buffer = createFlushByteBuf();
        } else if (buffer.readableBytes() + need > mtu) {
            output(buffer, this);
            buffer = createFlushByteBuf();
        }
        return buffer;
    }

    private ByteBuf createFlushByteBuf() {
        return byteBufAllocator.ioBuffer(this.mtu);
    }

    /**
     * user/upper level recv: returns size, returns below zero for EAGAIN
     *
     * 用户获取接收到数据(去除kcp头的用户数据)。该函数根据frg，把kcp包数据进行组合返回给用户。
     *
     * @param buf
     * @return
     */
    public int recv(ByteBuf buf) {
        if (rcvQueue.isEmpty()) {
            return -1;
        }
        int peekSize = peekSize();

        if (peekSize < 0) {
            return -2;
        }

        if (peekSize > buf.maxCapacity()) {
            return -3;
        }

        boolean recover = false;
        if (rcvQueue.size() >= rcvWnd) {
            recover = true;
        }

        // merge fragment
        int len = 0;
        for (Iterator<Segment> itr = rcvQueueItr.rewind(); itr.hasNext(); ) {
            Segment seg = itr.next();
            len += seg.data.readableBytes();
            buf.writeBytes(seg.data);

            int fragment = seg.frg;

            // log
            if (log.isDebugEnabled()) {
                log.debug("{} recv sn={}", this, seg.sn);
            }

            itr.remove();
            seg.recycle(true);

            if (fragment == 0) {
                break;
            }
        }

        assert len == peekSize;

        // move available data from rcv_buf -> rcv_queue
        moveRcvData();

        // fast recover
        if (rcvQueue.size() < rcvWnd && recover) {
            // ready to send back IKCP_CMD_WINS in ikcp_flush
            // tell remote my window size
            probe |= IKCP_ASK_TELL;
        }

        return len;
    }

    public int recv(List<ByteBuf> bufList) {
        if (rcvQueue.isEmpty()) {
            return -1;
        }
        int peekSize = peekSize();

        if (peekSize < 0) {
            return -2;
        }

        boolean recover = false;
        if (rcvQueue.size() >= rcvWnd) {
            recover = true;
        }

        // merge fragment
        int len = 0;
        for (Iterator<Segment> itr = rcvQueueItr.rewind(); itr.hasNext(); ) {
            Segment seg = itr.next();
            len += seg.data.readableBytes();
            bufList.add(seg.data);

            int fragment = seg.frg;

            // log
            if (log.isDebugEnabled()) {
                log.debug("{} recv sn={}", this, seg.sn);
            }

            itr.remove();
            seg.recycle(false);

            if (fragment == 0) {
                break;
            }
        }

        assert len == peekSize;

        // move available data from rcv_buf -> rcv_queue
        moveRcvData();

        // fast recover
        if (rcvQueue.size() < rcvWnd && recover) {
            // ready to send back IKCP_CMD_WINS in ikcp_flush
            // tell remote my window size
            probe |= IKCP_ASK_TELL;
        }

        return len;
    }

    public int peekSize() {
        if (rcvQueue.isEmpty()) {
            return -1;
        }

        Segment seg = rcvQueue.peek();
        if (seg.frg == 0) {
            return seg.data.readableBytes();
        }

        if (rcvQueue.size() < seg.frg + 1) { // Some segments have not arrived yet
            return -1;
        }

        int len = 0;
        for (Iterator<Segment> itr = rcvQueueItr.rewind(); itr.hasNext(); ) {
            Segment s = itr.next();
            len += s.data.readableBytes();
            if (s.frg == 0) {
                break;
            }
        }

        return len;
    }

    public boolean canRecv() {
        if (rcvQueue.isEmpty()) {
            return false;
        }

        Segment seg = rcvQueue.peek();
        if (seg.frg == 0) {
            return true;
        }

        if (rcvQueue.size() < seg.frg + 1) { // Some segments have not arrived yet
            return false;
        }

        return true;
    }

    /**
     * 发送数据到对端
     *
     * 把用户发送的数据根据MSS进行分片。用户发送1900字节的数据，MTU为1400byte。
     * 因此该函数会把1900byte的用户数据分成两个包，一个数据大小为1400，头frg设置
     * 为1，len设置为1400；第二个包，头frg设置为0，len设置为500。切好KCP包之后，
     * 放入到名为snd_queue的待发送队列中
     *
     * 注：流模式情况下，kcp会把两次发送的数据衔接为一个完整的kcp包。非流模式下，用
     * 户数据%MSS的包，也会作为一个包发送出去
     *
     * MTU，数据链路层规定的每一帧的最大长度，超过这个长度数据会被分片。通常MTU的长
     * 度为1500字节，IP协议规定所有的路由器均应该能够转发(512数据+60IP首部+4预留=
     * 576字节)的数据。MSS，最大输出大小(双方的约定)，KCP的大小为MTU-kcp头24字节。
     * IP数据报越短，路由器转发越快，但是资源利用率越低。传输链路上的所有MTU都一至的
     * 情况下效率最高，应该尽可能的避免数据传输的工程中，再次被分。UDP再次被分的后(
     * 通常1分为2)，只要丢失其中的任意一份，两份都要重新传输。因此，合理的MTU应该是
     * 保证数据不被再分的前提下，尽可能的大。
     *
     * 以太网的MTU通常为1500字节-IP头(20字节固定+40字节可选)-UDP头8个字节=1472字
     * 节。KCP会考虑多传输协议，但是在UDP的情况下，设置为1472字节更为合理。
     *
     * 主要工作
     * 1. 将数据包放入待发送队列
     */
    public int send(ByteBuf buf) {
        assert mss > 0;

        int len = buf.readableBytes();
        if (len == 0) {
            return -1;
        }

        // append to previous segment in streaming mode (if possible)
        // 流模式
        // 注：流模式情况下，kcp会把两次发送的数据衔接为一个完整的kcp包。
        // 非流模式下，用户数据%MSS的包，也会作为一个包发送出去。
        if (stream) {
            if (!sndQueue.isEmpty()) {
                Segment last = sndQueue.peekLast();
                ByteBuf lastData = last.data;
                int lastLen = lastData.readableBytes();
                if (lastLen < mss) {
                    int capacity = mss - lastLen;
                    int extend = Math.min(len, capacity);
                    if (lastData.maxWritableBytes() < extend) { // extend
                        ByteBuf newBuf = byteBufAllocator.ioBuffer(lastLen + extend);
                        newBuf.writeBytes(lastData);
                        lastData.release();
                        lastData = last.data = newBuf;
                    }
                    lastData.writeBytes(buf, extend);

                    len = buf.readableBytes();
                    if (len == 0) {
                        return 0;
                    }
                }
            }
        }

        int count = 0;
        if (len <= mss) {
            count = 1;
        } else {
            count = (len + mss - 1) / mss;
        }

        if (count >= IKCP_WND_RCV) { // Maybe don't need the condition in stream mode
            return -2;
        }

        if (count == 0) { // impossible
            count = 1;
        }

        // segment
        for (int i = 0; i < count; i++) {
            int size = Math.min(len, mss);
            Segment seg = Segment.createSegment(buf.readRetainedSlice(size));
            seg.frg = (short) (stream ? 0 : count - i - 1);
            sndQueue.add(seg);
            len = buf.readableBytes();
        }

        return 0;
    }

    /**
     * 标准方法（Jacobson / Karels 算法）
     *
     * 公式
     * SRTT = (1 -  α) * SRTT +  α * RTT
     * RTTVAR = (1 - β) * RTTVAR + β * (|RTT-SRTT|)
     * RTO= µ * SRTT + ∂ * RTTVar
     *
     * 权重因子 α 的建议值是 0.125
     * β 建议值 0.25
     * μ 建议值取 1，∂ 建议值取 4
     * 这种算法下 RTO 与 RTT 变化的差值关系更密切，能对变化剧烈的 RTT做出更及时的调整。
     */
    private void updateAck(int rtt) {
        if (rxSrtt == 0) {
            rxSrtt = rtt;
            rxRttvar = rtt / 2;
        } else {
            int delta = rtt - rxSrtt;
            if (delta < 0) {
                delta = -delta;
            }
            rxRttvar = (3 * rxRttvar + delta) / 4;
            rxSrtt = (7 * rxSrtt + rtt) / 8;
            if (rxSrtt < 1) {
                rxSrtt = 1;
            }
        }
        int rto = rxSrtt + Math.max(interval, 4 * rxRttvar);
        rxRto = ibound(rxMinrto, rto, IKCP_RTO_MAX);
    }

    /**
     * 更新 sndUna
     */
    private void shrinkBuf() {
        if (sndBuf.size() > 0) {
            Segment seg = sndBuf.peek();
            sndUna = seg.sn;
        } else {
            sndUna = sndNxt;
        }
    }

    private void parseAck(long sn) {
        // 产生重复确认的包，收到没发送过的序列号，概率极低，可能是conv没变重启程序导致的
        if (itimediff(sn, sndUna) < 0 || itimediff(sn, sndNxt) >= 0) {
            return;
        }

        for (Iterator<Segment> itr = sndBufItr.rewind(); itr.hasNext(); ) {
            Segment seg = itr.next();
            if (sn == seg.sn) {
                itr.remove();
                seg.recycle(true);
                break;
            }
            if (itimediff(sn, seg.sn) < 0) {
                break;
            }
        }
    }

    private void parseUna(long una) {
        for (Iterator<Segment> itr = sndBufItr.rewind(); itr.hasNext(); ) {
            Segment seg = itr.next();
            if (itimediff(una, seg.sn) > 0) {
                itr.remove();
                seg.recycle(true);
            } else {
                break;
            }
        }
    }

    private void parseFastack(long sn) {
        if (itimediff(sn, sndUna) < 0 || itimediff(sn, sndNxt) >= 0) {
            return;
        }

        for (Iterator<Segment> itr = sndBufItr.rewind(); itr.hasNext(); ) {
            Segment seg = itr.next();
            if (itimediff(sn, seg.sn) < 0) {
                break;
            } else if (sn != seg.sn) {
                seg.fastack++;
            }
        }
    }

    /**
     * KCP会把收到的数据包的sn及ts放置在acklist中，两个相邻的节点为一组，
     * 分别存储sn和ts。update时会读取acklist，并以IKCP_CMD_ACK的命令返
     * 回确认包。
     * @param sn
     * @param ts
     */
    private void ackPush(long sn, int ts) {
        int newSize = 2 * (ackcount + 1);

        if (newSize > acklist.length) {
            int newCapacity = acklist.length << 1; // double capacity

            if (newCapacity < 0) {
                throw new OutOfMemoryError();
            }

            int[] newArray = new int[newCapacity];
            System.arraycopy(acklist, 0, newArray, 0, acklist.length);
            this.acklist = newArray;
        }

        acklist[2 * ackcount] = (int) sn;
        acklist[2 * ackcount + 1] = ts;
        ackcount++;
    }

    private void parseData(Segment newSeg) {
        long sn = newSeg.sn;

        // kcp数据包放置rcv_buf队列。丢弃接收窗口之外的和重复的包
        if (itimediff(sn, rcvNxt + rcvWnd) >= 0 || itimediff(sn, rcvNxt) < 0) {
            newSeg.recycle(true);
            return;
        }

        boolean repeat = false;
        boolean findPos = false;
        ListIterator<Segment> listItr = null;
        if (rcvBuf.size() > 0) {
            listItr = rcvBufItr.rewind(rcvBuf.size());
            while (listItr.hasPrevious()) {
                Segment seg = listItr.previous();
                if (seg.sn == sn) {
                    repeat = true;
                    break;
                }
                if (itimediff(sn, seg.sn) > 0) {
                    findPos = true;
                    break;
                }
            }
        }

        if (repeat) {
            newSeg.recycle(true);
        } else if (listItr == null) {
            rcvBuf.add(newSeg);
        } else {
            if (findPos) {
                listItr.next();
            }
            listItr.add(newSeg);
        }

        // move available data from rcv_buf -> rcv_queue
        // 把rcv_buf中前面连续的数据全部移动至rcv_queue
        moveRcvData(); // Invoke the method only if the segment is not repeat?
    }

    private void moveRcvData() {
        for (Iterator<Segment> itr = rcvBufItr.rewind(); itr.hasNext(); ) {
            Segment seg = itr.next();
            if (seg.sn == rcvNxt && rcvQueue.size() < rcvWnd) {
                itr.remove();
                rcvQueue.add(seg);
                rcvNxt++;
            } else {
                break;
            }
        }
    }

    /**
     * 接收对端数据
     * KCP的接收过程是将UDP收到的数据进行解包，重新组装顺序的、可靠的数据后交付给用户。
     *
     * Part 1是解析包头、获取数据。 值得注意的是：
     * Part 1.3中，通过分析收到的远端数据中的una，确认了本机发送的哪些包被远端接收到了，从而可以将本机缓存在snd_buf中的（已被远端接收到的）发送数据清除掉。
     * 而Part 1.4中，则是通过分析远端单独回传的ack包，来确认本机哪个数据包被接收到，而从snd_buf中清除那个确认被接收到了的数据包。
     * Part 1.5中，则是本机接收远端数据包的主要逻辑部分。 在有剩余接收窗口的情况下，
     * 1.回传ack给远端，
     * 2.摒弃重复收到的数据包
     * 3.分析数据，放入rcv_buf中相对应的位置
     * Part 2是根据收到包头的信息，更新网络情况的统计数据，方便进行流控，同样的也不在本文中展开。
     */
    public InputStateEnum input(ByteBuf data) {
        long oldSndUna = sndUna;
        long maxack = 0;
        boolean flag = false;

        if (log.isDebugEnabled()) {
            log.debug("{} [RI] {} bytes", this, data.readableBytes());
        }

        if (data == null || data.readableBytes() < IKCP_OVERHEAD) {
            return InputStateEnum.NO_ENOUGH_HEAD;
        }
        // Part 1 逐步解析data中的数据
        while (true) {
            //kcp包对前面的24个字节进行解压，包括conv、frg、cmd、wnd、ts、sn、una、len。
            int conv, len, wnd, ts;
            long sn, una;
            byte cmd;
            short frg;
            Segment seg;

            if (data.readableBytes() < IKCP_OVERHEAD) {
                break;
            }

            conv = data.readIntLE();
            if (conv != this.conv && !(this.conv == 0 && autoSetConv)) {
                return InputStateEnum.INCONSISTENCY_CONV;
            }
            //** Part 1.1
            //** 解析出数据中的KCP头部
            cmd = data.readByte();
            frg = data.readUnsignedByte();
            wnd = data.readUnsignedShortLE();
            ts = data.readIntLE();
            sn = data.readUnsignedIntLE();
            una = data.readUnsignedIntLE();
            len = data.readIntLE();

            if (data.readableBytes() < len || len < 0) {
                return InputStateEnum.NO_ENOUGH_DATA;
            }

            if (cmd != IKCP_CMD_PUSH && cmd != IKCP_CMD_ACK && cmd != IKCP_CMD_WASK && cmd != IKCP_CMD_WINS) {
                return MISMATCH_CMD;
            }

            if (this.conv == 0 && autoSetConv) { // automatically set conv
                this.conv = conv;
            }
            //** Part 1.2
            //** 获得远端的窗口大小
            this.rmtWnd = wnd;
            //** Part 1.3
            //** 分析una，看哪些segment远端收到了，把远端收到的segment从snd_buf中移除
            //会删除snd_buf中，所有una之前的kcp数据包，因为这些数据包接收者已经确认。根据wnd更新接收端接收窗口大小
            parseUna(una);
            //** 因为snd_buf可能改变了，更新当前的snd_una
            shrinkBuf();

            boolean readed = false;
            int current = this.current;
            switch (cmd) {
                //** Part 1.4
                //** 如果收到的是远端发来的ACK包
                // IKCP_CMD_ACK数据确认包
                // 两个使命：1、RTO更新，2、确认发送包接收方已接收到。
                case IKCP_CMD_ACK: {
                    int rtt = itimediff(current, ts);
                    if (rtt >= 0) {
                        // 更新 RTO 因为此时收到远端的ack，所以我们知道远端的包到本机的时间，
                        // 因此可统计当前的网速如何，进行调整
                        updateAck(rtt);
                    }
                    //** 分析具体是哪个segment被收到了，将其从snd_buf中移除
                    //** 同时给snd_buf中的其它segment的fastack字段增加计数++
                    parseAck(sn);
                    //** 因为snd_buf可能改变了，更新当前的snd_una
                    shrinkBuf();
                    if (!flag) {
                        flag = true;
                        maxack = sn;
                    } else {
                        if (itimediff(sn, maxack) > 0) {
                            maxack = sn;
                        }
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("{} input ack: sn={}, rtt={}, rto={}", this, sn, rtt, rxRto);
                    }
                    break;
                }
                //** Part 1.5
                //** 如果收到的是远端发来的数据包
                // IKCP_CMD_PUSH数据发送命令
                // 解包，放入 rcv_buff
                case IKCP_CMD_PUSH: {
                    //** 如果还有足够多的接收窗口
                    if (itimediff(sn, rcvNxt + rcvWnd) < 0) {
                        //** push当前包的ack给远端（会在flush中发送ack出去)
                        ackPush(sn, ts);
                        //** 如果当前segment还没被接收过sn >= rcv_next
                        if (itimediff(sn, rcvNxt) >= 0) {
                            if (len > 0) {
                                seg = Segment.createSegment(data.readRetainedSlice(len));
                                readed = true;
                            } else {
                                seg = Segment.createSegment(byteBufAllocator, 0);
                            }
                            seg.conv = conv;
                            seg.cmd = cmd;
                            seg.frg = frg;
                            seg.wnd = wnd;
                            seg.ts = ts;
                            seg.sn = sn;
                            seg.una = una;
                            //** 1. 如果已经接收过了，则丢弃
                            //** 2. 否则将其按sn的顺序插入到rcv_buf中对应的位置中去
                            //** 3. 按顺序将sn连续在一起的segment转移转移到rcv_queue中
                            parseData(seg);
                        }
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("{} input push: sn={}, una={}, ts={}", this, sn, una, ts);
                    }
                    break;
                }
                //** Part 1.6
                //** 如果收到的包是远端发过来询问窗口大小的包
                // 告知发送 Window size 包大小
                case IKCP_CMD_WASK: {
                    // ready to send back IKCP_CMD_WINS in ikcp_flush
                    // tell remote my window size
                    //** 由于每个KCP包头都会包含窗口大小，所以此处其实只是个标记位而已，无须做其它事情
                    probe |= IKCP_ASK_TELL;
                    if (log.isDebugEnabled()) {
                        log.debug("{} input ask", this);
                    }
                    break;
                }
                case IKCP_CMD_WINS: {
                    // do nothing
                    if (log.isDebugEnabled()) {
                        log.debug("{} input tell: {}", this, wnd);
                    }
                    break;
                }
                default:
                    //** 不接受其它的命令
                    return MISMATCH_CMD;
            }

            if (!readed) {
                data.skipBytes(len);
            }
        }
        //Part 2 , 根据收到包头的信息，更新网络情况的统计数据，方便进行流控
        if (flag) {
            parseFastack(maxack);
        }

        if (itimediff(sndUna, oldSndUna) > 0) {
            if (cwnd < rmtWnd) {
                int mss = this.mss;
                if (cwnd < ssthresh) {
                    cwnd++;
                    incr += mss;
                } else {
                    if (incr < mss) {
                        incr = mss;
                    }
                    incr += (mss * mss) / incr + (mss / 16);
                    if ((cwnd + 1) * mss <= incr) {
                        cwnd++;
                    }
                }
                if (cwnd > rmtWnd) {
                    cwnd = rmtWnd;
                    incr = rmtWnd * mss;
                }
            }
        }

        return NORMAL;
    }

    /**
     * 可用接收窗口大小
     */
    private int wndUnused() {
        if (rcvQueue.size() < rcvWnd) {
            return rcvWnd - rcvQueue.size();
        }
        return 0;
    }

    /**
     * Part 1主要是发送ack包给远端，告诉远端自己收到了哪些数据包
     * Part 2主要是在远端接收窗口大小(rmt_wnd)为0时，发送窗口探测包给远端
     * Part 3 ~ Part 6就是发包的主体控制部分了，包括超时重传、何时重传等等
     * Part 7则是更新一些网络统计数据，用于流控的，不在此文中展开
     * ikcp_flush
     */
    private void flush() {
        int current = this.current;

        // 'ikcp_update' haven't been called.
        if (!updated) {
            return;
        }

        Segment seg = Segment.createSegment(byteBufAllocator, 0);
        seg.conv = conv;
        seg.cmd = IKCP_CMD_ACK;
        seg.frg = 0;
        seg.wnd = wndUnused();
        seg.una = rcvNxt;
        seg.sn = 0;
        seg.ts = 0;

        ByteBuf buffer = null;

        // flush acknowledges
        // Part 1 将前面收到数据时，压进ack发送队列的ack发送出去
        // ack 不用考虑远程窗口大小
        int count = ackcount;
        for (int i = 0; i < count; i++) {
            buffer = tryCreateOrOutput(buffer, IKCP_OVERHEAD);
            seg.sn = int2Uint(acklist[i * 2]);
            seg.ts = acklist[i * 2 + 1];
            encodeSeg(buffer, seg);
            if (log.isDebugEnabled()) {
                log.debug("{} flush ack: sn={}, ts={}", this, seg.sn, seg.ts);
            }
        }

        ackcount = 0;


        // probe window size (if remote window size equals zero)
        // 零窗口探测
        // Part 2 在远端窗口大小为0时，探测远端窗口大小
        // 当远端的接收窗口大小为0时，本机将不会再向远端发送数据，此时也就
        // 不会有远端的回传数据从而导致无法更新远端窗口大小。 因此需要单独
        // 的一类远端窗口大小探测包，在远端接收窗口大小为0时，隔一段时间询
        // 问一次，从而让本地有机会再开始重新传数据。
        if (rmtWnd == 0) {
            if (probeWait == 0) {
                probeWait = IKCP_PROBE_INIT;
                tsProbe = current + probeWait;
            } else {
                if (itimediff(current, tsProbe) >= 0) {
                    if (probeWait < IKCP_PROBE_INIT) {
                        probeWait = IKCP_PROBE_INIT;
                    }
                    probeWait += probeWait / 2;
                    if (probeWait > IKCP_PROBE_LIMIT) {
                        probeWait = IKCP_PROBE_LIMIT;
                    }
                    tsProbe = current + probeWait;
                    probe |= IKCP_ASK_SEND;
                }
            }
        } else {
            tsProbe = 0;
            probeWait = 0;
        }

        // flush window probing commands
        if ((probe & IKCP_ASK_SEND) != 0) {
            seg.cmd = IKCP_CMD_WASK;
            buffer = tryCreateOrOutput(buffer, IKCP_OVERHEAD);
            encodeSeg(buffer, seg);
            if (log.isDebugEnabled()) {
                log.debug("{} flush ask", this);
            }
        }

        // flush window probing commands
        if ((probe & IKCP_ASK_TELL) != 0) {
            seg.cmd = IKCP_CMD_WINS;
            buffer = tryCreateOrOutput(buffer, IKCP_OVERHEAD);
            encodeSeg(buffer, seg);
            if (log.isDebugEnabled()) {
                log.debug("{} flush tell: wnd={}", this, seg.wnd);
            }
        }

        probe = 0;

        // Part 3 计算窗口大小，以决定接下来是否继续发送数据
        // calculate window size
        int cwnd0 = Math.min(sndWnd, rmtWnd);
        if (!nocwnd) {
            // 取拥塞窗口和发送窗口最小值
            cwnd0 = Math.min(this.cwnd, cwnd0);
        }

        // Part 4 转移snd_queue中的数据到snd_buf中，以便后面发送出去
        // move data from snd_queue to snd_buf
        // 确保发送的数据不会让接收方的接收队列溢出
        while (itimediff(sndNxt, sndUna + cwnd0) < 0) {
            Segment newSeg = sndQueue.poll();
            if (newSeg == null) {
                break;
            }

            sndBuf.add(newSeg);

            newSeg.conv = conv;
            newSeg.cmd = IKCP_CMD_PUSH;
            newSeg.wnd = seg.wnd;
            newSeg.ts = current;
            newSeg.sn = sndNxt++;
            newSeg.una = rcvNxt;
            newSeg.resendts = current;
            newSeg.rto = rxRto;
            newSeg.fastack = 0;
            newSeg.xmit = 0;
        }

        // Part 5 计算重传时间
        // calculate resent
        int resent = fastresend > 0 ? fastresend : Integer.MAX_VALUE;
        // 超时发送 最小等待时间
        int rtomin = nodelay ? 0 : (rxRto >> 3);

        // Part 6 根据各个segment的发送情况发送segment
        // flush data segments
        int change = 0;
        boolean lost = false;
        for (Iterator<Segment> itr = sndBufItr.rewind(); itr.hasNext(); ) {
            Segment segment = itr.next();
            boolean needsend = false;
            // 第一次发送
            if (segment.xmit == 0) {
                needsend = true;
                incrXmit(segment);
                segment.rto = rxRto;
                // 数据包超时发送时间
                segment.resendts = current + segment.rto + rtomin;
                if (log.isDebugEnabled()) {
                    log.debug("{} flush data: sn={}, resendts={}", this, segment.sn, (segment.resendts - current));
                }
                // 超时发送
            } else if (itimediff(current, segment.resendts) >= 0) {
                needsend = true;
                incrXmit(segment);
                xmit++;
                segment.fastack = 0;
                if (!nodelay) {
                    segment.rto += rxRto;
                } else {
                    segment.rto += rxRto / 2;
                }
                segment.resendts = current + segment.rto;
                lost = true;
                if (log.isDebugEnabled()) {
                    log.debug("{} resend. sn={}, xmit={}, resendts={}", this, segment.sn, segment.xmit, (segment
                            .resendts - current));
                }
            } else if (segment.fastack >= resent) {
                if (segment.xmit <= fastlimit || fastlimit <= 0) {
                    needsend = true;
                    incrXmit(segment);
                    segment.fastack = 0;
                    segment.resendts = current + segment.rto;
                    change++;
                    if (log.isDebugEnabled()) {
                        log.debug("{} fastresend. sn={}, xmit={}, resendts={} ", this, segment.sn, segment.xmit,
                                (segment
                                .resendts - current));
                    }
                }
            }

            if (needsend) {
                segment.ts = current;
                segment.wnd = seg.wnd;
                segment.una = rcvNxt;

                ByteBuf segData = segment.data;
                int segLen = segData.readableBytes();
                int need = IKCP_OVERHEAD + segLen;

                buffer = tryCreateOrOutput(buffer, need);
                encodeSeg(buffer, segment);

                if (segLen > 0) {
                    // don't increases data's readerIndex, because the data may be resend.
                    buffer.writeBytes(segData, segData.readerIndex(), segLen);
                }

                if (segment.xmit >= deadLink) {
                    state = -1;
                }
            }
        }

        // flash remain segments
        if (buffer != null) {
            if (buffer.readableBytes() > 0) {
                output(buffer, this);
            } else {
                buffer.release();
            }
        }

        seg.recycle(true);

        // Part 7
        // update ssthresh
        if (change > 0) {
            int inflight = (int) (sndNxt - sndUna);
            ssthresh = inflight / 2;
            if (ssthresh < IKCP_THRESH_MIN) {
                ssthresh = IKCP_THRESH_MIN;
            }
            cwnd = ssthresh + resent;
            incr = cwnd * mss;
        }

        if (lost) {
            ssthresh = cwnd0 / 2;
            if (ssthresh < IKCP_THRESH_MIN) {
                ssthresh = IKCP_THRESH_MIN;
            }
            cwnd = 1;
            incr = mss;
        }

        if (cwnd < 1) {
            cwnd = 1;
            incr = mss;
        }
    }

    /**
     * update getState (call it repeatedly, every 10ms-100ms), or you can ask
     * ikcp_check when to call it again (without ikcp_input/_send calling).
     * 'current' - current timestamp in millisec.
     *
     * KCP会不停的进行update更新最新情况，数据的实际发送在update时进行
     *
     * @param current
     */
    public void update(int current) {
        this.current = current;

        if (!updated) {
            updated = true;
            tsFlush = this.current;
        }

        int slap = itimediff(this.current, tsFlush);

        if (slap >= 10000 || slap < -10000) {
            tsFlush = this.current;
            slap = 0;
        }

        /*if (slap >= 0) {
            tsFlush += setInterval;
            if (itimediff(this.current, tsFlush) >= 0) {
                tsFlush = this.current + setInterval;
            }
            flush();
        }*/

        if (slap >= 0) {
            tsFlush += interval;
            if (itimediff(this.current, tsFlush) >= 0) {
                tsFlush = this.current + interval;
            }
        } else {
            tsFlush = this.current + interval;
        }
        flush();
    }

    /**
     * Determine when should you invoke ikcp_update:
     * returns when you should invoke ikcp_update in millisec, if there
     * is no ikcp_input/_send calling. you can call ikcp_update in that
     * time, instead of call update repeatly.
     * Important to reduce unnacessary ikcp_update invoking. use it to
     * schedule ikcp_update (eg. implementing an epoll-like mechanism,
     * or optimize ikcp_update when handling massive kcp connections)
     *
     * @param current
     * @return
     */
    public int check(int current) {
        if (!updated) {
            return current;
        }

        int tsFlush = this.tsFlush;
        int slap = itimediff(current, tsFlush);
        if (slap >= 10000 || slap < -10000) {
            tsFlush = current;
            slap = 0;
        }

        if (slap >= 0) {
            return current;
        }

        int tmFlush = itimediff(tsFlush, current);
        int tmPacket = Integer.MAX_VALUE;

        for (Iterator<Segment> itr = sndBufItr.rewind(); itr.hasNext(); ) {
            Segment seg = itr.next();
            int diff = itimediff(seg.resendts, current);
            if (diff <= 0) {
                return current;
            }
            if (diff < tmPacket) {
                tmPacket = diff;
            }
        }

        int minimal = Math.min(tmPacket, tmFlush);
        if (minimal >= interval) {
            minimal = interval;
        }

        return current + minimal;
    }

    public boolean checkFlush() {
        if (ackcount > 0) {
            return true;
        }
        if (probe != 0) {
            return true;
        }
        if (sndBuf.size() > 0) {
            return true;
        }
        if (sndQueue.size() > 0) {
            return true;
        }
        return false;
    }

    private void incrXmit(Segment seg) {
        if (++seg.xmit > metric.maxSegXmit()) {
            metric.maxSegXmit(seg.xmit);
        }
    }

    public int getMtu() {
        return mtu;
    }

    public int setMtu(int mtu) {
        if (mtu < IKCP_OVERHEAD || mtu < 50) {
            return -1;
        }

        this.mtu = mtu;
        this.mss = mtu - IKCP_OVERHEAD;
        return 0;
    }

    public int getInterval() {
        return interval;
    }

    public int setInterval(int interval) {
        if (interval > 5000) {
            interval = 5000;
        } else if (interval < 10) {
            interval = 10;
        }
        this.interval = interval;

        return 0;
    }

    public int nodelay(boolean nodelay, int interval, int resend, boolean nc) {
        this.nodelay = nodelay;
        if (nodelay) {
            this.rxMinrto = IKCP_RTO_NDL;
        } else {
            this.rxMinrto = IKCP_RTO_MIN;
        }

        if (interval >= 0) {
            if (interval > 5000) {
                interval = 5000;
            } else if (interval < 10) {
                interval = 10;
            }
            this.interval = interval;
        }

        if (resend >= 0) {
            fastresend = resend;
        }

        this.nocwnd = nc;

        return 0;
    }

    /**
     * KCP默认为32，即可以接收最大为32*MTU=43.75kB。KCP采用update的方式，
     * 更新间隔为10ms，那么KCP限定了你最大传输速率为4375kB/s，在高网速传输
     * 大内容的情况下需要调用ikcp_wndsize调整接收与发送窗口
     */
    public int wndsize(int sndWnd, int rcvWnd) {
        if (sndWnd > 0) {
            this.sndWnd = sndWnd;
        }
        if (rcvWnd > 0) {
            this.rcvWnd = rcvWnd;
        }

        return 0;
    }

    public int waitSnd() {
        return this.sndBuf.size() + this.sndQueue.size();
    }

    public int getConv() {
        return conv;
    }

    public void setConv(int conv) {
        this.conv = conv;
    }

    public Object getUser() {
        return user;
    }

    public void setUser(Object user) {
        this.user = user;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public boolean isNodelay() {
        return nodelay;
    }

    public void setNodelay(boolean nodelay) {
        this.nodelay = nodelay;
        if (nodelay) {
            this.rxMinrto = IKCP_RTO_NDL;
        } else {
            this.rxMinrto = IKCP_RTO_MIN;
        }
    }

    public int getFastresend() {
        return fastresend;
    }

    public void setFastresend(int fastresend) {
        this.fastresend = fastresend;
    }

    public int getFastlimit() {
        return fastlimit;
    }

    public void setFastlimit(int fastlimit) {
        this.fastlimit = fastlimit;
    }

    public boolean isNocwnd() {
        return nocwnd;
    }

    public void setNocwnd(boolean nocwnd) {
        this.nocwnd = nocwnd;
    }

    public int getRxMinrto() {
        return rxMinrto;
    }

    public void setRxMinrto(int rxMinrto) {
        this.rxMinrto = rxMinrto;
    }

    public int getRcvWnd() {
        return rcvWnd;
    }

    public void setRcvWnd(int rcvWnd) {
        this.rcvWnd = rcvWnd;
    }

    public int getSndWnd() {
        return sndWnd;
    }

    public void setSndWnd(int sndWnd) {
        this.sndWnd = sndWnd;
    }

    public boolean isStream() {
        return stream;
    }

    public void setStream(boolean stream) {
        this.stream = stream;
    }

    public int getDeadLink() {
        return deadLink;
    }

    public void setDeadLink(int deadLink) {
        this.deadLink = deadLink;
    }

    public void setByteBufAllocator(ByteBufAllocator byteBufAllocator) {
        this.byteBufAllocator = byteBufAllocator;
    }

    public boolean isAutoSetConv() {
        return autoSetConv;
    }

    public void setAutoSetConv(boolean autoSetConv) {
        this.autoSetConv = autoSetConv;
    }

    int getSrtt() {
        return rxSrtt;
    }

    int getRttvar() {
        return rxRttvar;
    }

    int getRto() {
        return rxRto;
    }

    long getSndNxt() {
        return sndNxt;
    }

    long getSndUna() {
        return sndUna;
    }

    long getRcvNxt() {
        return rcvNxt;
    }

    int getCwnd() {
        return cwnd;
    }

    int getXmit() {
        return xmit;
    }

    public KcpMetric getMetric() {
        return metric;
    }

    @Override
    public String toString() {
        return "Kcp(" +
                "conv=" + conv +
                ')';
    }

}
