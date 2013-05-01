/*
 * Copyright (c) 2008-2012 Vrije Universiteit, The Netherlands All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the Vrije Universiteit nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS ``AS IS''
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package interdroid.vdb.transport;

import ibis.smartsockets.SmartSocketsProperties;
import ibis.smartsockets.naming.NameResolver;
import ibis.smartsockets.util.MalformedAddressException;
import ibis.smartsockets.virtual.InitializationException;
import ibis.smartsockets.virtual.VirtualSocket;
import ibis.smartsockets.virtual.VirtualSocketAddress;
import interdroid.vdb.content.VdbProviderRegistry;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.eclipse.jgit.errors.NotSupportedException;
import org.eclipse.jgit.errors.TransportException;
import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.BasePackFetchConnection;
import org.eclipse.jgit.transport.BasePackPushConnection;
import org.eclipse.jgit.transport.FetchConnection;
import org.eclipse.jgit.transport.PackTransport;
import org.eclipse.jgit.transport.PacketLineIn;
import org.eclipse.jgit.transport.PacketLineOut;
import org.eclipse.jgit.transport.PushConnection;
import org.eclipse.jgit.transport.TcpTransport;
import org.eclipse.jgit.transport.Transport;
import org.eclipse.jgit.transport.TransportProtocol;
import org.eclipse.jgit.transport.URIish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A jGit TcpTransport built on top of SmartSockets.
 *
 * @author nick &lt;palmer@cs.vu.nl&gt;
 *
 */
public class SmartSocketsTransport extends TcpTransport
implements PackTransport {
	/**
	 * Access to logger.
	 */
	private static final Logger LOG = LoggerFactory
			.getLogger(SmartSocketsTransport.class);

	/**
	 * The scheme used by this transport.
	 */
	public static final String SMARTSOCKETS_TRANSPORT_SCHEME = "ss";

	/**
	 * The port smartsockets works on.
	 */
	private static final int SMARTSOCKETS_PORT = 9090;

	/**
	 * The properties for the smart sockets subsystem.
	 */
	private static Properties sSocketProperties = new Properties();

	static {
		sSocketProperties.put(SmartSocketsProperties.DIRECT_CACHE_IP, "false");

		sSocketProperties.put(SmartSocketsProperties.START_HUB, "true");
	}

	/**
	 * The protocol used to speak over this transport.
	 */
	public static final TransportProtocol PROTO = new TransportProtocol() {
		public String getName() {
			return "Smart Sockets";
		}

		public Set<String> getSchemes() {
			return Collections.singleton(
					SMARTSOCKETS_TRANSPORT_SCHEME); //$NON-NLS-1$
		}

		public Set<URIishField> getRequiredFields() {
			return Collections.unmodifiableSet(
					EnumSet.of(URIishField.HOST, URIishField.USER,
							URIishField.PATH));
		}

		public Set<URIishField> getOptionalFields() {
			return Collections.unmodifiableSet(EnumSet.of(URIishField.PORT));
		}

		public int getDefaultPort() {
			return SMARTSOCKETS_PORT;
		}

		public Transport open(final URIish uri, final Repository local,
				final String remoteName) throws NotSupportedException {
			return new SmartSocketsTransport(local, uri);
		}
	};

	/** The real name of the user. */
	public static final String REAL_NAME = "name";
	/** The email of the user. */
	public static final String EMAIL = "email";

	// TODO: This should come from a property or something
	/** The timeout for connections. */
	private static final int TIMEOUT = 1000 * 30;

	/**
	 * Construct a transport for the given repository and URI.
	 * @param local the local repository
	 * @param uri the uri to transport with
	 */
	protected SmartSocketsTransport(final Repository local, final URIish uri) {
		super(local, uri);
	}

	/**
	 * Construct a transport for the given URI.
	 * @param uri the uri to transport with
	 */
	public SmartSocketsTransport(final URIish uri) {
		super(null, uri);
	}

	@Override
	public final FetchConnection openFetch() throws TransportException {
		try {
			return new SmartSocketsFetchConnection();
		} catch (Exception e) {
			throw new TransportException(uri, "Unable to open socket.", e);
		}
	}

	@Override
	public final PushConnection openPush() throws TransportException {
		try {
			return new SmartSocketsPushConnection();
		} catch (Exception e) {
			throw new TransportException(uri, "Unable to open socket.", e);
		}
	}

	@Override
	public void close() {
		// TODO: Unregister our endpoint with smartsockets?
	}

	/**
	 * Open a connection to the given URI with the given timeout.
	 * @param uri the uri to open
	 * @param timeout the timeout for the operation
	 * @return a VirtualSocket connected to the requested URI
	 * @throws IOException if reading, writing or address resolution fails
	 * @throws InitializationException if we are unable to initialize
	 */
	static VirtualSocket openConnection(final URIish uri, final int timeout)
			throws IOException,
			InitializationException {
		LOG.debug("Opening connection to: {} {}", uri, timeout);
		LOG.debug("Resolving: {}@{}", uri.getUser(), uri.getHost());
		NameResolver resolver = getResolver();
		VirtualSocketAddress otherSide = resolver.resolve(
				uri.getUser() + "@" + uri.getHost(), timeout);
		LOG.debug("Other side is: {}", otherSide);
		if (otherSide == null) {
			throw new IOException("Unable to resolve host: "
					+ uri.getUser() + "@" + uri.getHost());
		}
		VirtualSocket s = resolver.getSocketFactory()
				.createClientSocket(otherSide, timeout, null);
		return s;
	}

	/**
	 * Request the given service.
	 * @param name the name of the service to connect
	 * @param pckOut the packet line out to send with
	 * @throws IOException if reading or writing fails.
	 */
	final void service(final String name, final PacketLineOut pckOut)
			throws IOException {
		final StringBuilder cmd = new StringBuilder();
		cmd.append(name);
		cmd.append(' ');
		cmd.append(uri.getPath());
		cmd.append('\0');
		cmd.append("host=");
		cmd.append(uri.getHost());
		cmd.append("user=");
		cmd.append(uri.getHost());
		if (uri.getPort() > 0 && uri.getPort() != SMARTSOCKETS_PORT) {
			cmd.append(':');
			cmd.append(uri.getPort());
		}
		cmd.append('\0');
		pckOut.writeString(cmd.toString());
		pckOut.flush();
	}

	/**
	 * The connection used to fetch packets from another host.
	 * @author nick &lt;palmer@cs.vu.nl&gt;
	 *
	 */
	final class SmartSocketsFetchConnection extends BasePackFetchConnection {
		/** The socket the connection talks over. */
		private VirtualSocket sock;

		/**
		 * Construct a new connection.
		 * @throws IOException if reading or writing fails
		 * @throws InitializationException if we fail to initialize
		 */
		SmartSocketsFetchConnection()
				throws IOException, InitializationException {
			super(SmartSocketsTransport.this);
			sock = openConnection(uri, getTimeout());
			try {
				InputStream sIn = sock.getInputStream();
				OutputStream sOut = sock.getOutputStream();

				sIn = new BufferedInputStream(sIn);
				sOut = new BufferedOutputStream(sOut);

				init(sIn, sOut);
				service("git-upload-pack", pckOut);
			} catch (IOException err) {
				close();
				throw new TransportException(uri,
						JGitText.get().remoteHungUpUnexpectedly, err);
			}
			readAdvertisedRefs();
		}

		@Override
		public void close() {
			super.close();

			if (sock != null) {
				try {
					sock.close();
				} catch (IOException err) {
					LOG.warn("Exception while closing.", err);
				} finally {
					sock = null;
				}
			}
		}
	}

	/**
	 * The connection used to push packs to another client.
	 * @author nick &lt;palmer@cs.vu.nl&gt;
	 *
	 */
	class SmartSocketsPushConnection extends BasePackPushConnection {
		/** The virtual socket the connection talks over. */
		private VirtualSocket sock;

		/**
		 * Construct a push connection.
		 * @throws IOException if reading or writing
		 * @throws InitializationException if initialization fails
		 */
		SmartSocketsPushConnection()
				throws IOException, InitializationException {
			super(SmartSocketsTransport.this);
			sock = openConnection(uri, getTimeout());
			try {
				InputStream sIn = sock.getInputStream();
				OutputStream sOut = sock.getOutputStream();

				sIn = new BufferedInputStream(sIn);
				sOut = new BufferedOutputStream(sOut);

				init(sIn, sOut);
				service("git-receive-pack", pckOut);
			} catch (IOException err) {
				close();
				throw new TransportException(uri,
						JGitText.get().remoteHungUpUnexpectedly, err);
			}
			readAdvertisedRefs();
		}

		@Override
		public void close() {
			super.close();

			if (sock != null) {
				try {
					sock.close();
				} catch (IOException err) {
					LOG.warn("Exception while closing.", err);
				} finally {
					sock = null;
				}
			}
		}
	}

	/**
	 * @param localEmail the local email address
	 * @param remoteEmail the remote email address
	 * @return the list of local repositories visible to the remote user.
	 * @throws IOException if reading or writing fails
	 * @throws InitializationException if we fail to initialize
	 */
	public static List<Map<String, Object>> getRepositories(
			final String localEmail, final String remoteEmail)
					throws IOException,
					InitializationException {
		List<Map<String, Object>> repositories = null;
		URIish uri;
		LOG.debug("Getting repositories from: " + remoteEmail);
		try {
			uri = new URIish(SMARTSOCKETS_TRANSPORT_SCHEME
					+ "://" + remoteEmail + "/");
		} catch (URISyntaxException e) {
			throw new MalformedAddressException(e);
		}
		VirtualSocket socket = openConnection(uri, TIMEOUT);
		socket.setSoTimeout(TIMEOUT);

		PacketLineOut out = null;
		PacketLineIn in = null;
		try {
			LOG.debug("Sending Command.");
			out = new PacketLineOut(socket.getOutputStream());
			out.writeString("git-list-repos " + localEmail);

			repositories = new ArrayList<Map<String, Object>>();
			LOG.debug("Reading result.");
			in = new PacketLineIn(socket.getInputStream());
			for (String repository = in.readString();
					!repository.equals(PacketLineIn.END);
					repository = in.readString()) {
				LOG.debug("Read repository: {}", repository);
				Map<String, Object> repoInfo = new HashMap<String, Object>();
				repoInfo.put(VdbProviderRegistry.REPOSITORY_NAME, repository);
				repoInfo.put(VdbProviderRegistry.REPOSITORY_IS_PEER,
						Boolean.valueOf(in.readString()));
				repoInfo.put(VdbProviderRegistry.REPOSITORY_IS_PUBLIC,
						Boolean.valueOf(in.readString()));
				repositories.add(repoInfo);
			}
		} finally {
			if (in != null) {
				socket.getInputStream().close();
			}
			if (out != null) {
				socket.getOutputStream().close();
			}
			socket.close();
		}
		LOG.debug("Done fetching repositories.");
		return repositories;
	}

	/**
	 * @return the name resolver used by the transport
	 * @throws InitializationException if the name resolver fails to initialize
	 */
	public static NameResolver getResolver() throws InitializationException {
		return NameResolver.getOrCreateResolver(
				SMARTSOCKETS_TRANSPORT_SCHEME, sSocketProperties, true);
	}

	/**
	 * Add a hub to the transport.
	 * @param hub the hub addresses to be added
	 * @throws InitializationException if smartsockets fails to initialize.
	 */
	public static void addHub(final String... hub)
			throws InitializationException {
		getResolver().addHub(hub);
	}

}
