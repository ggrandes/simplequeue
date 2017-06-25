package org.javastack.simplequeue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.javastack.kvstore.KVStoreFactory;
import org.javastack.kvstore.holders.IntHolder;
import org.javastack.kvstore.holders.LongHolder;
import org.javastack.kvstore.io.FileStreamStore;
import org.javastack.kvstore.io.StringSerializer;
import org.javastack.kvstore.structures.btree.BplusTree.InvalidDataException;
import org.javastack.kvstore.structures.btree.BplusTree.TreeEntry;
import org.javastack.kvstore.structures.btree.BplusTreeFile;

public class QueueManagerPersisted<T> implements QueueManager<T>, Runnable {
	private static final Logger log = Logger.getLogger(QueueManagerPersisted.class);
	private static final int CLEAR_ELEMENTS = 1024;
	private static final IntHolder FIRST_KEY = IntHolder.valueOf(1);
	private static final IntHolder RESET_KEY = IntHolder.valueOf(CLEAR_ELEMENTS << 2);

	private final LinkedHashMap<String, DiskQueue<T>> queues;;
	private final File storeDir;
	private final QueueSerializer<T> serializer;
	private Thread t = null;
	private volatile boolean running = false;

	public QueueManagerPersisted(final File storeDir, final QueueSerializer<T> serializer,
			final int maxConcurrentOpenedQueues) throws IOException {
		this.storeDir = storeDir.getCanonicalFile();
		this.serializer = serializer;
		this.queues = allocQueues(maxConcurrentOpenedQueues);
	}

	@Override
	public boolean checkQName(final String qname) {
		final int len = qname.length();
		if (len > (255 - 10)) // Most filesystem are limited to 255
			return false;
		// For safety only [A-Za-z0-9._-] are allowed
		for (int i = 0; i < len; i++) {
			final char c = qname.charAt(i);
			if ((c >= 'A') && (c <= 'Z')) // [A-Z]
				continue;
			if ((c >= 'a') && (c <= 'z')) // [a-z]
				continue;
			if ((c >= '0') && (c <= '9')) // [0-9]
				continue;
			switch (c) { // [._-]
				case '.':
				case '_':
				case '-':
					continue;
			}
			return false; // Invalid char found
		}
		return true;
	}

	private LinkedHashMap<String, DiskQueue<T>> allocQueues(final int maxConcurrentOpenedQueues) {
		return new LinkedHashMap<String, DiskQueue<T>>() {
			private static final long serialVersionUID = 42L;

			@Override
			protected boolean removeEldestEntry(final java.util.Map.Entry<String, DiskQueue<T>> eldest) {
				final boolean doRemove = (size() > maxConcurrentOpenedQueues);
				if (doRemove) {
					eldest.getValue().close();
				}
				return doRemove;
			}
		};
	}

	private synchronized DiskQueue<T> getQueue(final String qname) {
		DiskQueue<T> queue = queues.get(qname);
		if (queue == null) {
			try {
				queue = new DiskQueue<T>(storeDir, qname, serializer);
				queue.open();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			queues.put(qname, queue);
		}
		return queue;
	}

	@Override
	public T get(final String qname, final long timeout, final TimeUnit unit, final TransactionAdapter<T> ta)
			throws InterruptedException, TimeoutException {
		final DiskQueue<T> queue = getQueue(qname);
		final T e = queue.poll(timeout, unit, ta);
		if (e == null)
			throw new TimeoutException();
		return e;
	}

	@Override
	public void put(final String qname, final T element, final long timeout, final TimeUnit unit)
			throws InterruptedException, TimeoutException {
		final DiskQueue<T> queue = getQueue(qname);
		if (!queue.offer(element, timeout, unit))
			throw new TimeoutException();
	}

	@Override
	public int size(final String qname) {
		final DiskQueue<T> queue = getQueue(qname);
		return queue.size();
	}

	@Override
	public synchronized void init() throws InterruptedException {
		if (!storeDir.exists()) {
			if (!storeDir.mkdirs())
				throw new RuntimeException(new IOException("Invalid storeDir: " + storeDir));
		}
		if (!running) {
			t = new Thread(this, this.getClass().getSimpleName() + ":datasync");
			t.start();
		}
	}

	@Override
	public synchronized void destroy() throws InterruptedException {
		running = false;
		for (final DiskQueue<T> s : queues.values()) {
			s.close();
		}
		queues.clear();
	}

	public synchronized void sync() {
		for (final DiskQueue<T> s : queues.values()) {
			s.sync();
		}
	}

	@Override
	public void run() {
		try {
			running = true;
			while (running) {
				sync();
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		} finally {
			running = false;
			t = null;
		}
	}

	private static class DiskQueue<T> {
		private static final int BUF_LEN = 0x10000;
		private final KVStoreFactory<IntHolder, LongHolder> fac = new KVStoreFactory<IntHolder, LongHolder>(
				IntHolder.class, LongHolder.class);
		private final BplusTreeFile<IntHolder, LongHolder> map;
		private final FileStreamStore stream;
		private final ByteBuffer wbuf, rbuf;
		private final QueueSerializer<T> serializer;
		private final String qname;
		private IntHolder maxKey = null;
		private boolean isDirty = false;

		public DiskQueue(final File storeDir, final String qname, final QueueSerializer<T> serializer)
				throws InstantiationException, IllegalAccessException, IOException {
			if (!storeDir.exists()) {
				throw new IOException("Invalid storeDir: " + storeDir);
			}
			final File storeTree = new File(storeDir, qname + ".tree");
			final File storeStream = new File(storeDir, qname + ".stream");
			wbuf = ByteBuffer.allocate(BUF_LEN);
			rbuf = ByteBuffer.allocate(BUF_LEN);
			map = fac.createTreeFile(
					fac.createTreeOptionsDefault().set(KVStoreFactory.FILENAME, storeTree.getCanonicalPath())
							.set(KVStoreFactory.DISABLE_POPULATE_CACHE, true));
			stream = new FileStreamStore(storeStream, BUF_LEN);
			stream.setAlignBlocks(true);
			stream.setFlushOnWrite(true);
			stream.setSyncOnFlush(false);
			this.serializer = serializer;
			this.qname = qname;
		}

		public synchronized void open() throws InvalidDataException {
			try {
				if (map.open())
					log.info("open tree ok");
			} catch (InvalidDataException e) {
				log.error("open tree error, recovery needed");
				if (map.recovery(false) && map.open()) {
					log.info("recovery ok, tree opened");
				} else {
					throw e;
				}
			}
			stream.open();
			size();
			this.maxKey = nextKey(getLastKey());
		}

		private static final IntHolder nextKey(final IntHolder maxKey) {
			if (maxKey == null) {
				return FIRST_KEY;
			}
			final int n = maxKey.intValue() + 1;
			return ((n < Integer.MAX_VALUE) ? IntHolder.valueOf(n) : FIRST_KEY);
		}

		public synchronized void sync() {
			if (isDirty) {
				if ((size() == 0) && (maxKey.intValue() > CLEAR_ELEMENTS)) {
					stream.clear();
					map.clear();
					maxKey = nextKey(null);
				} else {
					stream.sync();
					map.sync();
				}
				isDirty = false;
			}
		}

		public synchronized void close() {
			if (map.isEmpty()) {
				stream.delete();
				map.delete();
			} else {
				stream.close();
				map.close();
			}
		}

		public synchronized boolean offer(final T value, final long timeout, final TimeUnit unit) {
			boolean notify = false;
			if (size() == 0)
				notify = true;
			final boolean ret = put(maxKey, value);
			maxKey = nextKey(maxKey);
			isDirty = true;
			if (notify)
				notifyAll();
			return ret;
		}

		public synchronized T poll(final long timeout, final TimeUnit unit, final TransactionAdapter<T> ta) {
			try {
				final long begin = System.currentTimeMillis();
				final long howWait = unit.toMillis(timeout);
				final long expire = begin + howWait;
				long now;
				while ((size() == 0) && ((now = System.currentTimeMillis()) < expire)) {
					wait(expire - now);
				}
				final int size = size();
				if (size > 0) {
					final TreeEntry<IntHolder, LongHolder> entry = map.firstEntry();
					final LongHolder offset = entry.getValue();
					final T value = get(offset);
					final boolean canCommit;
					if (ta != null) {
						canCommit = ta.canCommit(qname, value);
					} else {
						canCommit = true;
					}
					if (canCommit) {
						remove(entry.getKey());
					}
					isDirty = true;
					if ((size == 1) && (maxKey.intValue() > CLEAR_ELEMENTS)) {
						maxKey = nextKey(RESET_KEY);
					}
					return value;
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return null;
		}

		public synchronized int size() {
			return map.size();
		}

		private IntHolder getLastKey() {
			IntHolder k = map.lastKey();
			if (k == null)
				return null;
			return k;
		}

		private boolean put(final IntHolder k, final T v) {
			wbuf.clear();
			serializer.serialize(v, wbuf);
			wbuf.flip();
			final long offset = stream.write(wbuf);
			return map.put(k, LongHolder.valueOf(offset));
		}

		private T get(final LongHolder offset) {
			if (offset == null)
				return null;
			rbuf.clear();
			stream.read(offset.longValue(), rbuf);
			return serializer.deserialize(rbuf);
		}

		private void remove(final IntHolder k) {
			map.remove(k);
		}
	}

	public static interface QueueSerializer<T> {
		public static final QueueSerializer<String> STRING_SERIALIZER = new QueueSerializer<String>() {
			@Override
			public void serialize(final String value, final ByteBuffer wbuf) {
				StringSerializer.fromStringToBuffer(wbuf, value);
			}

			@Override
			public String deserialize(final ByteBuffer rbuf) {
				return StringSerializer.fromBufferToString(rbuf);
			}
		};
		public static final QueueSerializer<byte[]> BYTEARRAY_SERIALIZER = new QueueSerializer<byte[]>() {
			@Override
			public void serialize(final byte[] value, final ByteBuffer wbuf) {
				if (value == null) {
					wbuf.putInt(Integer.MIN_VALUE);
					return;
				}
				final int len = value.length;
				wbuf.putInt(len);
				wbuf.put(value, 0, len);
			}

			@Override
			public byte[] deserialize(final ByteBuffer rbuf) {
				final int len = rbuf.getInt();
				if (len == Integer.MIN_VALUE) {
					return null;
				}
				final byte[] bytes = new byte[len];
				rbuf.get(bytes, 0, len);
				return bytes;
			}
		};

		/**
		 * Serialize data into writebuf
		 * 
		 * @param data
		 * @param writebuf
		 */
		public void serialize(final T data, final ByteBuffer writebuf);

		/**
		 * Deserialize data from readbuf
		 * 
		 * @param readbuf
		 * @return
		 */
		public T deserialize(final ByteBuffer readbuf);
	}

	public static void main(final String[] args) throws Throwable {
		final QueueManagerPersisted<byte[]> qmgr = new QueueManagerPersisted<byte[]>(new File("/tmp"),
				QueueSerializer.BYTEARRAY_SERIALIZER, 16);
		qmgr.init();
		try {
			for (int i = 0; i < 3; i++)
				qmgr.put("test", ("ola k ase" + i).getBytes(), 5000, TimeUnit.MILLISECONDS);
			System.out.println(qmgr.size("test"));
			byte[] v = null;
			while ((v = qmgr.get("test", 5000, TimeUnit.MILLISECONDS, null)) != null)
				System.out.println(new String(v));
		} catch (TimeoutException e) {
		} finally {
			qmgr.destroy();
		}
	}
}
