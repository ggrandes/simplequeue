/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.javastack.simplequeue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.javastack.simplequeue.QueueManager.TransactionAdapter;

/**
 * Simple Queue in HTTP
 * 
 * @author Guillermo Grandes / guillermo.grandes[at]gmail.com
 */
public class SimpleQueueServlet extends HttpServlet {
	private static final String CONF_FILE = "/simplequeue.properties";
	private static final String STORAGE_PARAM = "org.javastack.simplequeue.directory";
	private static final String CONCURRENT_QUEUES_PARAM = "org.javastack.simplequeue.concurrent.queues";
	private static final String DEFAULT_TIMEOUT_PARAM = "org.javastack.simplequeue.default.timeout";

	private static final long serialVersionUID = 42L;
	private static final Logger log = Logger.getLogger(SimpleQueueServlet.class);
	private static final Charset DEFAULT_URL_ENCODING = Charset.forName("UTF-8");
	private static final int MAX_LENGTH = 65500;

	private File storeDir = null;
	private int maxConcurrentOpenedQueues;
	private int defaultTimeout;

	private QueueManager<byte[]> qmgr = null;

	public SimpleQueueServlet() {
	}

	@Override
	public void init() throws ServletException {
		// Get Config param
		try {
			storeDir = new File(getConfig(STORAGE_PARAM, null)).getCanonicalFile();
			log.info("Storage Path: " + storeDir);
			storeDir.mkdirs();
			maxConcurrentOpenedQueues = Integer.valueOf(getConfig(CONCURRENT_QUEUES_PARAM, "128"));
			log.info("Max Concurrent Opened Queues: " + maxConcurrentOpenedQueues);
			defaultTimeout = Integer.valueOf(getConfig(DEFAULT_TIMEOUT_PARAM, "1000"));
			log.info("Default Timeout: " + defaultTimeout);
		} catch (Exception e) {
			throw new ServletException(e);
		}
		// Init QueueManager
		try {
			final File dir = getContextStoreDir(storeDir);
			qmgr = new QueueManagerPersisted<byte[]>(dir,
					QueueManagerPersisted.QueueSerializer.BYTEARRAY_SERIALIZER, maxConcurrentOpenedQueues);
			qmgr.init();
		} catch (Exception e) {
			throw new ServletException(e);
		}
	}

	private final String getConfig(final String paramName, final String defValue) throws ServletException {
		String value = null;
		// Try Context Property
		if (value == null) {
			try {
				value = getServletContext().getInitParameter(paramName);
			} catch (Exception e) {
			}
		}
		// Try System Property
		if (value == null) {
			try {
				value = System.getProperty(paramName);
			} catch (Exception e) {
			}
		}
		// Try System Environment
		if (value == null) {
			try {
				value = System.getenv(paramName);
			} catch (Exception e) {
			}
		}
		// Try Config file
		if (value == null) {
			final Properties p = new Properties();
			InputStream is = null;
			try {
				log.info("Searching " + CONF_FILE.substring(1) + " in classpath");
				is = this.getClass().getResourceAsStream(CONF_FILE);
				if (is != null) {
					p.load(is);
					is.close();
				}
			} catch (Exception e) {
				throw new ServletException(e);
			}
			log.info("Searching " + paramName + " in config file");
			value = p.getProperty(paramName);
		}
		// Default value
		if (value == null) {
			value = defValue;
		}
		// Throw Error
		if (value == null) {
			throw new ServletException("Invalid param for: " + paramName);
		}
		return value;
	}

	private final File getContextStoreDir(final File dir) throws IOException {
		final String ctxPath = getServletContext().getContextPath();
		String cn = ctxPath;
		if (cn.isEmpty()) {
			cn = "ROOT";
		} else {
			if (cn.charAt(0) == '/') {
				cn = cn.substring(1);
			}
			cn = cn.replaceAll("[^A-Za-z0-9._-]", "_");
		}
		return new File(dir, cn);
	}

	@Override
	public void destroy() {
		try {
			qmgr.destroy();
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		qmgr = null;
	}

	@Override
	protected void doPut(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		final int bodySize = request.getContentLength();
		final String qname = getPathInfoKey(request.getPathInfo());
		final int timeout = Math.max(getParameter(request, "timeout", defaultTimeout), 1);
		if (qname == null) {
			sendResponse(response, HttpServletResponse.SC_BAD_REQUEST, "NO_QUEUE");
			return;
		}

		final String type = mapNull(request.getContentType(), "").toLowerCase();
		byte[] data = null;
		if (type.equals("application/x-www-form-urlencoded")) {
			final String pdata = request.getParameter("data");
			if (pdata == null) {
				sendResponse(response, HttpServletResponse.SC_BAD_REQUEST, "BAD_DATA_WWW-FORM-URLENCODING");
				return;
			}
			data = pdata.getBytes(DEFAULT_URL_ENCODING);
		} else if (type.startsWith("multipart/")) {
			sendResponse(response, HttpServletResponse.SC_BAD_REQUEST, "UNSUPPORTED_ENCODING");
			return;
		} else if (type.isEmpty() || type.startsWith("application/") || type.startsWith("text/")) {
			final ServletInputStream is = request.getInputStream();
			if (bodySize < 0) {
				// Transfer-Encoding: chunked
				final byte[] cbuf = new byte[512];
				final ArrayList<Chunk> clist = new ArrayList<Chunk>(128); // 512x128=65536bytes max
				int clen = 0, maxlen = 0;
				while ((clen = is.read(cbuf)) >= 0) {
					if ((clen > 0) && (maxlen < MAX_LENGTH)) {
						clist.add(new Chunk(cbuf, clen));
						maxlen += clen;
					}
				}
				if (maxlen > MAX_LENGTH) {
					sendResponse(response, HttpServletResponse.SC_BAD_REQUEST, "TOO_LONG");
					return;
				} else {
					final byte[] bbuf = new byte[maxlen];
					int offset = 0, chunks = clist.size();
					for (int i = 0; i < chunks; i++) {
						final Chunk chunk = clist.set(i, null);
						System.arraycopy(chunk.buf, 0, bbuf, offset, chunk.len);
						offset += chunk.len;
					}
					data = bbuf;
				}
			} else {
				if (bodySize > MAX_LENGTH) {
					sendResponse(response, HttpServletResponse.SC_BAD_REQUEST, "TOO_LONG");
					return;
				} else {
					final byte[] bbuf = new byte[bodySize];
					int len = 0, offset = 0;
					while ((len = is.read(bbuf, offset, bodySize - offset)) >= 0) {
						offset += len;
					}
					if (offset != bodySize) {
						sendResponse(response, HttpServletResponse.SC_BAD_REQUEST,
								"CONTENT_LENGTH_MISMATCH:" + bodySize + ":" + offset);
						return;
					}
					data = bbuf;
				}
			}
		}

		if (data == null) {
			sendResponse(response, HttpServletResponse.SC_BAD_REQUEST, "BAD_REQUEST");
			return;
		}

		try {
			qmgr.put(qname, data, timeout, TimeUnit.MILLISECONDS);
			sendResponse(response, HttpServletResponse.SC_OK, "OK");
		} catch (TimeoutException e) {
			sendResponse(response, HttpServletResponse.SC_NO_CONTENT, "TIMEOUT");
		} catch (InterruptedException e) {
			sendResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "INTERRUPTED");
		} catch (Exception e) {
			log.error("Exception: " + e, e);
			sendResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "ERROR");
		}
	}

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		final String qname = getPathInfoKey(request.getPathInfo());
		final int timeout = Math.max(getParameter(request, "timeout", defaultTimeout), 1);
		final boolean reqSize = (request.getParameter("size") != null);
		final String forceMime = mapNull(request.getParameter("forceMime"), "");
		if (qname == null) {
			sendResponse(response, HttpServletResponse.SC_BAD_REQUEST, "NO_QUEUE");
			return;
		}
		try {
			if (reqSize) {
				sendResponse(response, HttpServletResponse.SC_OK, Integer.toString(qmgr.size(qname)));
			} else {
				final TransactionAdapter<byte[]> ta = new TransactionAdapter<byte[]>() {
					@Override
					public boolean canCommit(final String qname, final byte[] data) {
						try {
							sendResponse(response, HttpServletResponse.SC_OK, data, forceMime);
							return true;
						} catch (IOException e) {
							log.error("IOException: " + e);
						} catch (Exception e) {
							log.error("Exception: " + e, e);
						}
						return false;
					}
				};
				qmgr.get(qname, timeout, TimeUnit.MILLISECONDS, ta);
			}
		} catch (TimeoutException e) {
			sendResponse(response, HttpServletResponse.SC_NO_CONTENT, "TIMEOUT");
		} catch (InterruptedException e) {
			sendResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "INTERRUPTED");
		} catch (Exception e) {
			log.error("Exception: " + e, e);
			sendResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "ERROR");
		}
	}

	private static final void setNoCache(final HttpServletResponse response) {
		response.setHeader("Cache-Control", "private, no-cache, no-store");
		response.setHeader("Pragma", "no-cache");
	}

	private final void sendResponse(final HttpServletResponse response, final int code, final byte[] data,
			final String forceMime) throws IOException {
		final ServletOutputStream os = response.getOutputStream();
		response.setStatus(code);
		setNoCache(response);
		response.setContentLength(data.length);
		String mime = "application/octet-stream";
		if (forceMime != null) {
			final String newMime = getServletContext().getMimeType("." + forceMime);
			if (newMime != null) {
				mime = newMime;
			}
		}
		response.setContentType(mime);
		os.flush(); // Flush Headers
		os.write(data);
		os.flush(); // Flush Body (here throws exception if client is dead)
	}

	private final void sendResponse(final HttpServletResponse response, final int code, final String msg)
			throws IOException {
		if (response.isCommitted()) {
			log.error("Unable to sendResponse error code=" + code + " msg=" + msg + " already commited");
			return;
		}
		final PrintWriter out = response.getWriter();
		response.setStatus(code);
		setNoCache(response);
		if (code != HttpServletResponse.SC_OK) {
			response.setHeader("X-Extended-Info", msg);
		}
		if (code != HttpServletResponse.SC_NO_CONTENT) {
			response.setContentLength(msg.length());
			response.setContentType("text/plain");
			out.print(msg);
		}
		out.flush();
	}

	private static final String mapNull(final String value, final String def) {
		if (value == null)
			return def;
		return value;
	}

	private static final int getParameter(final HttpServletRequest req, final String param, final int def) {
		final String p = req.getParameter(param);
		if (p == null)
			return def;
		return Integer.parseInt(p);
	}

	private final String getPathInfoKey(final String pathInfo) {
		if (pathInfo == null)
			return null;
		if (pathInfo.isEmpty())
			return null;
		final String key = pathInfo.substring(1);
		if (!qmgr.checkQName(key))
			return null;
		return key;
	}

	private static final class Chunk {
		public final byte[] buf;
		public final int len;

		public Chunk(final byte[] buf, final int len) {
			this.buf = buf;
			this.len = len;
		}
	}
}
