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

import ibis.smartsockets.naming.NameResolver;
import ibis.smartsockets.virtual.InitializationException;
import ibis.smartsockets.virtual.VirtualServerSocket;
import ibis.smartsockets.virtual.VirtualSocket;
import ibis.smartsockets.virtual.VirtualSocketAddress;
import ibis.smartsockets.virtual.VirtualSocketFactory;
import interdroid.vdb.content.VdbProviderRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.pack.PackConfig;
import org.eclipse.jgit.transport.PacketLineIn;
import org.eclipse.jgit.transport.PacketLineOut;
import org.eclipse.jgit.transport.ReceivePack;
import org.eclipse.jgit.transport.ServiceMayNotContinueException;
import org.eclipse.jgit.transport.UploadPack;
import org.eclipse.jgit.transport.resolver.ReceivePackFactory;
import org.eclipse.jgit.transport.resolver.RepositoryResolver;
import org.eclipse.jgit.transport.resolver.ServiceNotAuthorizedException;
import org.eclipse.jgit.transport.resolver.ServiceNotEnabledException;
import org.eclipse.jgit.transport.resolver.UploadPackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Basic daemon for the anonymous <code>git://</code> transport protocol. */
public class SmartSocketsDaemon {
	/**
	 * The service which receives packs from clients.
	 *
	 * @author nick &lt;palmer@cs.vu.nl&gt;
	 *
	 */
	private final class ReceiveDaemonService extends SmartsocketsDaemonService {
		{
			setEnabled(false);
		}

		/**
		 * Construct this service.
		 */
		private ReceiveDaemonService() {
			super("receive-pack", "receivepack");
		}

		@Override
		protected void execute(final SmartSocketsDaemonClient dc,
				final Repository db) throws IOException,
				ServiceNotEnabledException,
				ServiceNotAuthorizedException {
			ReceivePack rp = mReceivePackFactory.create(dc, db);
			InputStream in = dc.getInputStream();
			OutputStream out = dc.getOutputStream();
			rp.receive(in, out, null);
		}
	}

	/**
	 * The service which uploads packs to clients.
	 *
	 * @author nick &lt;palmer@cs.vu.nl&gt;
	 *
	 */
	private final class UploadDaemonService extends SmartsocketsDaemonService {
		{
			setEnabled(true);
		}

		/**
		 * Construct the service.
		 */
		private UploadDaemonService() {
			super("upload-pack", "uploadpack");
		}

		@Override
		protected void execute(final SmartSocketsDaemonClient dc,
				final Repository db) throws IOException,
				ServiceNotEnabledException,
				ServiceNotAuthorizedException {
			UploadPack up = mUploadPackFactory.create(dc, db);
			InputStream in = dc.getInputStream();
			OutputStream out = dc.getOutputStream();
			up.upload(in, out, null);
		}
	}

	/**
	 * The service which lists repositories for clients.
	 *
	 * @author nick &lt;palmer@cs.vu.nl&gt;
	 *
	 */
	private final class ListDaemonService extends SmartsocketsDaemonService {
		{
			setEnabled(true);
			setOverridable(false);
		}

		/**
		 * Construct the service.
		 */
		private ListDaemonService() {
			super("list-repos", "listrepos");
		}

		@Override
		protected void execute(final SmartSocketsDaemonClient dc,
				final String commandLine) throws IOException,
				ServiceNotEnabledException,
				ServiceNotAuthorizedException {
			LOG.debug("List Repos Called");
			PacketLineIn in = null;
			PacketLineOut out = null;
			final String email = commandLine.substring(
					getCommandName().length() + 1);
			LOG.debug("Listing repos for: {}", email);
			try {
				in = new PacketLineIn(dc.getInputStream());
				out = new PacketLineOut(dc.getOutputStream());
				List<Map<String, Object>> repositories =
						((VdbRepositoryResolver<SmartSocketsDaemonClient>)
								mRepositoryResolver).getRepositoryList(email);
				for (int i = 0; i < repositories.size(); i++) {
					Map<String, Object> repo = repositories.get(i);
					if (Boolean.TRUE.equals(repo.get(
							VdbProviderRegistry.REPOSITORY_IS_PUBLIC))
							|| Boolean.TRUE.equals(repo.get(
									VdbProviderRegistry.REPOSITORY_IS_PEER))) {
						LOG.debug("Sending repo: {}", repo.get(
								VdbProviderRegistry.REPOSITORY_NAME));
						out.writeString((String) repo.get(
								VdbProviderRegistry.REPOSITORY_NAME));
						out.writeString(String.valueOf(repo.get(
								VdbProviderRegistry.REPOSITORY_IS_PEER)));
						out.writeString(String.valueOf(repo.get(
								VdbProviderRegistry.REPOSITORY_IS_PUBLIC)));
					}
				}
				out.end();
			} finally {
				if (in != null) {
					try {
						dc.getInputStream().close();
					} catch (IOException e) {
						LOG.warn("Ignoring exception while closing.");
					}
				}
				if (out != null) {
					try {
						dc.getOutputStream().close();
					} catch (IOException e) {
						LOG.warn("Ignoring exception while closing.");
					}
				}
			}
		}

		@Override
		void execute(final SmartSocketsDaemonClient client,
				final Repository db)
				throws IOException, ServiceNotEnabledException,
				ServiceNotAuthorizedException {
			LOG.error("Got request for non-db enabled request.");
		}
	}

	/**
	 * Factory for creating upload packs.
	 *
	 * @author nick &lt;palmer@cs.vu.nl&gt;
	 *
	 */
	private final class SmartSocketsUploadPackFactory implements
			UploadPackFactory<SmartSocketsDaemonClient> {
		/**
		 * Constructs an upload pack for the client and repo.
		 * @param req the request
		 * @param db the repository
		 * @throws ServiceNotEnabledException if the service is not enabled
		 * @throws ServiceNotAuthorizedException if the user is not authorized
		 * @return the UploadPack for the client and repo.
		 */
		public UploadPack create(final SmartSocketsDaemonClient req,
				final Repository db)
				throws ServiceNotEnabledException,
				ServiceNotAuthorizedException {
			UploadPack up = new UploadPack(db);
			up.setTimeout(getTimeout());
			up.setPackConfig(getPackConfig());
			return up;
		}
	}

	/**
	 * Factory for creating recieve packs.
	 *
	 * @author nick &lt;palmer@cs.vu.nl&gt;
	 *
	 */
	private final class SmartSocketsRecievePackFactory implements
			ReceivePackFactory<SmartSocketsDaemonClient> {
		/**
		 * Constructs an upload pack for the client and repo.
		 * @param req the request
		 * @param db the repository
		 * @throws ServiceNotEnabledException if the service is not enabled
		 * @throws ServiceNotAuthorizedException if the user is not authorized
		 * @return the UploadPack for the client and repo.
		 */
		public ReceivePack create(final SmartSocketsDaemonClient req,
				final Repository db)
				throws ServiceNotEnabledException,
				ServiceNotAuthorizedException {
			ReceivePack rp = new ReceivePack(db);

			SocketAddress peer = req.getRemoteAddress();
			String name = "anonymous";
			String email = "anonymous@" + peer.toString();
			rp.setRefLogIdent(new PersonIdent(name, email));
			rp.setTimeout(getTimeout());

			return rp;
		}
	}

	/**
	 * Access to logger.
	 */
	private static final Logger LOG = LoggerFactory
			.getLogger(SmartSocketsDaemon.class);

	/** 9418: IANA assigned port number for Git. */
	public static final int DEFAULT_PORT = 2525;

	/** TCP socket backlog. */
	private static final int BACKLOG = 5;

	/** The local address. */
	private VirtualSocketAddress mLocalAddress;

	/** The dameon services. */
	private final SmartsocketsDaemonService[] mServices;

	/** The processors handling requests. */
	private final ThreadGroup mProcessors;

	/** True if we are running. */
	private boolean mRun;

	/** The thread accepting connections. */
	private Thread mAcceptThread;

	/** Socket timeout. */
	private int mTimeout;

	/** The pack configuration for exchanging packs. */
	private PackConfig mPackConfig;

	/** The resolver which converts requests into repositories. */
	private volatile RepositoryResolver<SmartSocketsDaemonClient>
	mRepositoryResolver;

	/** The upload pack handler. */
	private volatile UploadPackFactory<SmartSocketsDaemonClient>
	mUploadPackFactory;

	/** The receive pack handler. */
	private volatile ReceivePackFactory<SmartSocketsDaemonClient>
	mReceivePackFactory;

	/**
	 * The listen socket.
	 */
	private VirtualServerSocket mListenSock;
	/**
	 * The factory we are using to build sockets.
	 */
	private VirtualSocketFactory mSocketFactory;
	/**
	 * The name resolver we use to do lookups.
	 */
	private NameResolver mResolver;

	/** Configure a daemon to listen on any available network port. */
	public SmartSocketsDaemon() {
		this(null);
	}

	/**
	 * Configure a new daemon for the specified network address.
	 *
	 * @param addr
	 *            address to listen for connections on. If null, any available
	 *            port will be chosen on all network interfaces.
	 */
	@SuppressWarnings("unchecked")
	public SmartSocketsDaemon(final VirtualSocketAddress addr) {
		mLocalAddress = addr;
		mProcessors = new ThreadGroup("Git-Daemon");

		mRepositoryResolver = (RepositoryResolver<SmartSocketsDaemonClient>)
				RepositoryResolver.NONE;

		mUploadPackFactory = new SmartSocketsUploadPackFactory();

		mReceivePackFactory =
				new SmartSocketsRecievePackFactory();

		mServices = new SmartsocketsDaemonService[] {
				new ListDaemonService(),
				new UploadDaemonService(),
				new ReceiveDaemonService() };
	}

	/** @return the address connections are received on. */
	public final synchronized VirtualSocketAddress getAddress() {
		return mLocalAddress;
	}

	/**
	 * Lookup a supported service so it can be reconfigured.
	 *
	 * @param serviceName name of the service;
	 *             e.g. "receive-pack"/"git-receive-pack"
	 *             or "upload-pack"/"git-upload-pack".
	 * @return the service; null if this daemon implementation doesn't support
	 *         the requested service type.
	 */
	public final synchronized SmartsocketsDaemonService getService(
			final String serviceName) {
		String name = serviceName;
		LOG.debug("Looking for service: {}", name);
		if (!name.startsWith("git-")) {
			name = "git-" + name;
		}
		for (final SmartsocketsDaemonService s : mServices) {
			if (s.getCommandName().equals(name)) {
				return s;
			}
		}
		return null;
	}

	/** @return timeout (in seconds) before aborting an IO operation. */
	public final int getTimeout() {
		return mTimeout;
	}

	/**
	 * Set the timeout before willing to abort an IO call.
	 *
	 * @param seconds
	 *            number of seconds to wait (with no data transfer occurring)
	 *            before aborting an IO read or write operation with the
	 *            connected client.
	 */
	public final void setTimeout(final int seconds) {
		mTimeout = seconds;
	}

	/** @return configuration controlling packing, may be null. */
	public final PackConfig getPackConfig() {
		return mPackConfig;
	}

	/**
	 * Set the configuration used by the pack generator.
	 *
	 * @param pc
	 *            configuration controlling packing parameters. If null the
	 *            source repository's settings will be used.
	 */
	public final void setPackConfig(final PackConfig pc) {
		this.mPackConfig = pc;
	}

	/**
	 * Set the resolver used to locate a repository by name.
	 *
	 * @param resolver
	 *            the resolver instance.
	 */
	public final void setRepositoryResolver(
			final RepositoryResolver<SmartSocketsDaemonClient> resolver) {
		mRepositoryResolver = resolver;
	}

	/**
	 * Set the factory to construct and configure per-request UploadPack.
	 *
	 * @param factory
	 *            the factory. If null upload-pack is disabled.
	 */
	@SuppressWarnings("unchecked")
	public final void setUploadPackFactory(
			final UploadPackFactory<SmartSocketsDaemonClient> factory) {
		if (factory != null) {
			mUploadPackFactory = factory;
		} else {
			mUploadPackFactory = (UploadPackFactory<SmartSocketsDaemonClient>)
					UploadPackFactory.DISABLED;
		}
	}

	/**
	 * Set the factory to construct and configure per-request ReceivePack.
	 *
	 * @param factory
	 *            the factory. If null receive-pack is disabled.
	 */
	@SuppressWarnings("unchecked")
	public final void setReceivePackFactory(
			final ReceivePackFactory<SmartSocketsDaemonClient> factory) {
		if (factory != null) {
			mReceivePackFactory = factory;
		} else {
			mReceivePackFactory =
					(ReceivePackFactory<SmartSocketsDaemonClient>)
					ReceivePackFactory.DISABLED;
		}
	}

	/**
	 * Start this daemon on a background thread.
	 *
	 * @throws IOException
	 *             the server socket could not be opened.
	 * @throws InitializationException
	 *             the daemon is already running or fails to initialize
	 */
	public final synchronized void start()
			throws IOException, InitializationException {
		LOG.debug("Starting SmartSocketsDaemon.");
		if (mAcceptThread != null) {
			throw new InitializationException(
					JGitText.get().daemonAlreadyRunning);
		}

		// Start the resolver if it isn't already running.
		mResolver = SmartSocketsTransport.getResolver();
		mSocketFactory = mResolver.getSocketFactory();
		LOG.debug("Socket Factory has serviceLink: {}",
				mSocketFactory.getServiceLink());

		mListenSock = mSocketFactory.createServerSocket(
				DEFAULT_PORT, BACKLOG, null);
		if (mListenSock == null) {
			throw new InitializationException(
					"Unable to construct server socket.");
		}
		mLocalAddress = mListenSock.getLocalSocketAddress();

		mRun = true;
		mAcceptThread = new Thread(mProcessors, "Git-Daemon-Accept") {
			public void run() {
				while (isRunning()) {
					try {
						LOG.debug("Waiting for new connection...");
						startClient(mListenSock.accept());
						LOG.debug("Accepted connection");
					} catch (InterruptedIOException e) {
						LOG.warn("Interrupted while accepting.", e);
					} catch (IOException e) {
						break;
					}
				}
				LOG.debug("No longer running.");
				try {
					LOG.debug("Closing listen socket.");
					mListenSock.close();
					LOG.debug("Listen socket closed.");
				} catch (IOException err) {
					LOG.warn("Ignored while closing socket: ", err);
				}
			}
		};
		mAcceptThread.start();
	}

	/** @return true if this daemon is receiving connections. */
	public final synchronized boolean isRunning() {
		return mRun;
	}

	/** Stop this daemon. */
	public final synchronized void stop() {
		LOG.info("Stopping SmartSocketsDaemon.");
		NameResolver.closeAllResolvers();
		if (mAcceptThread != null) {
			synchronized (this) {
				mRun = false;
				try {
					LOG.debug("Closing accept socket.");
					mListenSock.close();
					mAcceptThread.interrupt();
					//mSocketFactory.end();
				} catch (Exception e1) {
					LOG.warn("Ignored while closing accept socket.", e1);
				}
			}
			LOG.debug("Joining accept thread.");
			try {
				mAcceptThread.join();
			} catch (InterruptedException e) {
				LOG.warn("Ignored while joining accept thread.", e);
			}
			LOG.debug("Joined accept thread.");
			mAcceptThread = null;
		}
	}

	/**
	 * Start a client handler on the given socket.
	 * @param virtualSocket the virtual socket for this client.
	 */
	private void startClient(final VirtualSocket virtualSocket) {
		LOG.debug("Starting client for: {}", virtualSocket);
		final SmartSocketsDaemonClient dc = new SmartSocketsDaemonClient(this);

		final SocketAddress peer = virtualSocket.getRemoteSocketAddress();
		dc.setRemoteAddress(peer);

		new Thread(mProcessors, "Git-Daemon-Client " + peer.toString()) {
			public void run() {
				try {
					LOG.debug("Executing client request:{}", dc);
					dc.execute(virtualSocket);
					LOG.debug("Client request handled.");
				} catch (Exception e) {
					LOG.warn("Exception while servicing client ignored.", e);
				} finally {
					LOG.debug("Closing streams");
					try {
						virtualSocket.getInputStream().close();
					} catch (IOException e) {
						LOG.warn("Exception while closing input stream: ", e);
					}
					try {
						virtualSocket.getOutputStream().close();
					} catch (IOException e) {
						LOG.warn("Exception while closing output stream: ", e);
					}
					LOG.debug("Closing socket.");
					try {
						virtualSocket.close();
					} catch (IOException e) {
						LOG.warn("Ignored while closing socket",
								e);
					}
				}
				LOG.debug("Thread is complete.");
			}
		}
		.start();
		LOG.debug("Launched client thread.");
	}

	/**
	 * @param cmd the command
	 * @return the service which handles the command.
	 */
	final synchronized SmartsocketsDaemonService matchService(
			final String cmd) {
		for (final SmartsocketsDaemonService d : mServices) {
			if (d.handles(cmd)) {
				return d;
			}
		}
		return null;
	}

	/**
	 * Opens the given repository for the given client.
	 * @param client the client to open for
	 * @param serviceName the repository to open.
	 * @return the Git repository.
	 */
	final Repository openRepository(
			final SmartSocketsDaemonClient client, final String serviceName) {
		// Assume any attempt to use \ was by a Windows client
		// and correct to the more typical / used in Git URIs.
		//
		String name = serviceName.replace('\\', '/');

		// git://thishost/path should always be name="/path" here
		//
		if (name.charAt(0) != '/') {
			return null;
		}

		try {
			return mRepositoryResolver.open(client, name.substring(1));
		} catch (RepositoryNotFoundException e) {
			// null signals it "wasn't found", which is all that is suitable
			// for the remote client to know.
			return null;
		} catch (ServiceNotAuthorizedException e) {
			// null signals it "wasn't found", which is all that is suitable
			// for the remote client to know.
			return null;
		} catch (ServiceNotEnabledException e) {
			// null signals it "wasn't found", which is all that is suitable
			// for the remote client to know.
			return null;
		} catch (ServiceMayNotContinueException e) {
			// null signals it "wasn't found", which is all that is suitable
			// for the remote client to know.
			return null;
		}
	}

	/**
	 * @return the name resolver we are using.
	 */
	public final NameResolver getResolver() {
		return mResolver;
	}
}
