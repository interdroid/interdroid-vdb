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
package interdroid.vdb.persistence.impl;

import interdroid.vdb.content.DatabaseInitializer;
import interdroid.vdb.content.avro.AvroContentProvider;
import interdroid.vdb.content.metadata.Metadata;
import interdroid.vdb.persistence.api.DirtyCheckoutException;
import interdroid.vdb.persistence.api.MergeInProgressException;
import interdroid.vdb.persistence.api.MergeInfo;
import interdroid.vdb.persistence.api.VdbCheckout;
import interdroid.vdb.persistence.api.VdbInitializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.avro.Schema;
import org.eclipse.jgit.api.AddCommand;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.NoFilepatternException;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import android.database.sqlite.SQLiteDatabase;

/**
 * This class implements a checkout of a repository.
 *
 * @author nick &lt;palmer@cs.vu.nl&gt;
 *
 */
public class VdbCheckoutImpl implements VdbCheckout {
	// TODO: Should come from R?
	private static final String INITIAL_SCHEMA_ONLY_VERSION = "Initial schema-only version.";

	// TODO: Should come from R?
	public static final String VDB_EMAIL = "vd@localhost";

	// TODO: Should come from R?
	public static final String VERSIONING_DAEMON = "Versioning Daemon";

	/**
	 * Access to logger.
	 */
	private static final Logger LOG = LoggerFactory
			.getLogger(VdbCheckoutImpl.class);

	/**
	 * The name of the file we store the schema in.
	 */
	private static final String SCHEMA_FILE = "schema";

	/**
	 * The prefix for a branch reference.
	 */
	private static final String BRANCH_REF_PREFIX = Constants.R_HEADS;
	/**
	 * The name of the database file.
	 */
	private static final String SQLITEDB = "sqlite.db";
	/**
	 * The name of the merge info file.
	 */
	private static final String MERGEINFO = "MERGE_INFO";

	/**
	 * The timeout for attempting to get the checkout lock.
	 */
	private static final int LOCK_TIMEOUT = 5;

	/**
	 * The VDB repository.
	 */
	private final VdbRepositoryImpl mVdbRepository;
	/**
	 * The jGit repository.
	 */
	private final Repository mGitRepository;
	/**
	 * The name of the checkout.
	 */
	private final String mCheckoutName;
	/**
	 * The directory the checkout lives in.
	 */
	private final File mDirectory;
	/**
	 * The current merge state of this checkout.
	 */
	private MergeInfo mMergeInfo;
	/**
	 * The database for this checkout.
	 */
	private SQLiteDatabase mDb;
	/**
	 * A flag indicating this checkout was deleted.
	 */
	private boolean mDeleted;
	/**
	 * A flag indicating this checkout is read only.
	 */
	private boolean mReadOnly;

	/**
	 * We protect access to the sqlite database by using this lock.
	 * The read/write lock DOES NOT correspond to reading or writing
	 * the database.
	 *
	 * Instead - the read lock is used for accessing the database both
	 * for ro or rw modes, while the write lock is used for exclusively
	 * locking the checkout directory for commits.
	 */
	private final ReentrantReadWriteLock mLock
	= new ReentrantReadWriteLock();

	private SQLiteDatabase mUpdateDb;

	/**
	 * Construct a checkout.
	 * @param parentRepo the repository for this checkout
	 * @param checkoutName the name of the checkout
	 */
	public VdbCheckoutImpl(final VdbRepositoryImpl parentRepo,
			final String checkoutName) {
		this(parentRepo, checkoutName, false);
	}

	/**
	 * Construct a (possibly read only) checkout.
	 * @param parentRepo the repository for this checkout
	 * @param checkoutName the name of the checkout
	 * @param readOnly is this checkout read only
	 */
	public VdbCheckoutImpl(final VdbRepositoryImpl parentRepo,
			final String checkoutName, final boolean readOnly) {
		mVdbRepository = parentRepo;
		mCheckoutName = checkoutName;
		mDirectory = new File(parentRepo.getRepositoryDir(), checkoutName);
		mGitRepository = parentRepo.getGitRepository(checkoutName);
		mReadOnly = readOnly;

		if (!mDirectory.isDirectory()) { // assume it's already checked out
			throw new RuntimeException("Not checked out yet.");
		}
		loadMergeInfo();
	}

	/**
	 * Commit the current state to the repository.
	 * @param authorName the name of the author
	 * @param authorEmail the email for the author
	 * @param msg the message for this commit
	 * @throws IOException if reading or writing fails
	 * @throws MergeInProgressException if the merge is not complete
	 */
	@Override
	public final synchronized void commit(final String authorName,
			final String authorEmail, final String msg)
					throws IOException, MergeInProgressException {
		checkDeletedState();
		checkReadOnly();
		if (LOG.isDebugEnabled()) {
			LOG.debug("commit on " + mCheckoutName);
		}

		if (mMergeInfo != null && !mMergeInfo.isResolved()) {
			throw new MergeInProgressException();
		}

		try {
			if (!mLock.writeLock().tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
				throw new RuntimeException(
						"Timeout waiting for the locked database for commit.");
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		try {
			commitImpl(authorName, authorEmail, msg);
		} finally {
			mLock.writeLock().unlock();
		}
	}

	/**
	 * Checks if this is read only.
	 */
	private void checkReadOnly() {
		if (mReadOnly) {
			throw new RuntimeException("Checkout is reado nly");
		}
	}

	/**
	 * The implementation of the commit operation.
	 * @param authorName the name of the author
	 * @param authorEmail the authors email
	 * @param msg the commit message
	 * @throws IOException if reading or writing fails
	 * @throws MergeInProgressException if a merge is not resolved
	 */
	private synchronized void commitImpl(final String authorName,
			final String authorEmail, final String msg)
					throws IOException, MergeInProgressException {

		if (mMergeInfo != null && !mMergeInfo.isResolved()) {
			throw new MergeInProgressException();
		}

		Git git = new Git(mGitRepository);
		CommitCommand commit = git.commit();
		AddCommand add = git.add();
		add.addFilepattern(SQLITEDB);
		add.addFilepattern(SCHEMA_FILE);
		commit.setMessage(msg);
		try {
			add.call();
		} catch (NoFilepatternException e) {
			throw new IOException();
		} catch (GitAPIException e) {
		    throw new IOException();
		}
		PersonIdent author = new PersonIdent(authorName, authorEmail);
		commit.setAuthor(author);
		commit.setCommitter(author);
		RevCommit revision;
		try {
			revision = commit.call();
			RefUpdate update = mGitRepository.updateRef(mCheckoutName);
			update.setNewObjectId(revision);
			update.setRefLogIdent(author);
			update.setRefLogMessage(msg, false);
			update.update();
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}

		if (mMergeInfo != null) {
			// successfully committed the merge, get back to normal mode
			mMergeInfo = null;
			saveMergeInfo();
			detachMergeDatabases();
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Succesfully committed revision "
					+ revision.getName().toString() + " on branch "
					+ mCheckoutName);
		}
	}

	/**
	 * Creates the master checkout for a repository.
	 * @param parentRepo the repository
	 * @param initializer the initializer for the database
	 * @return the checkout of the master
	 * @throws IOException if reading or writing fail
	 */
	public static VdbCheckoutImpl createMaster(
			final VdbRepositoryImpl parentRepo,
			final VdbInitializer initializer)
					throws IOException {
		VdbCheckoutImpl branch = null;

		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating master for: " + parentRepo.getName());
		}
		File masterDir = new File(parentRepo.getRepositoryDir(),
				Constants.MASTER);
		if (!masterDir.mkdirs()) {
			throw new IOException("Unable to create directory: "
					+ masterDir.getCanonicalPath());
		}

		if (initializer != null) {
			SQLiteDatabase db = SQLiteDatabase.openOrCreateDatabase(
					new File(masterDir, SQLITEDB), null);
			db.setVersion(1);
			initializer.onCreate(db);

			File schema = new File(masterDir, SCHEMA_FILE);
			if (!schema.createNewFile()) {
				throw new RuntimeException("Unable to create schema file");
			}
			FileOutputStream fos = new FileOutputStream(schema);
			fos.write(initializer.getSchema().getBytes("utf8"));
			fos.close();

			branch = new VdbCheckoutImpl(parentRepo, Constants.MASTER);
			branch.setDb(db);

			try {
				branch.commit(VERSIONING_DAEMON, VDB_EMAIL,
						INITIAL_SCHEMA_ONLY_VERSION);
			} catch (MergeInProgressException e) {
				// should never happen because we're surely not in merge mode
				throw new RuntimeException(e);
			}
		}

		return branch;
	}

	/**
	 * Sets the database.
	 * @param db the db to set to
	 */
	private synchronized void setDb(final SQLiteDatabase db) {
		mDb = db;
	}

	/**
	 * Opens the database.
	 */
	private synchronized void openDatabase() {
		if (mDb == null) {
			mDb = SQLiteDatabase.openDatabase(
					new File(mDirectory, SQLITEDB).getAbsolutePath(),
					null /* cursor factory */,
					SQLiteDatabase.OPEN_READWRITE);
			try {
				attachMergeDatabases();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * Detaches the merge databases.
	 */
	private synchronized void detachMergeDatabases() {
		mDb.execSQL("DETACH DATABASE base");
		mDb.execSQL("DETACH DATABASE ours");
		mDb.execSQL("DETACH DATABASE theirs");
	}

	/**
	 * Attaches the merge databases.
	 * @throws IOException if reading or writing fail.
	 */
	private synchronized void attachMergeDatabases() throws IOException {
		openDatabase();

		MergeInfo mergeInfo = getMergeInfo();
		if (mergeInfo != null) {

			File baseCheckout =
					mVdbRepository.checkoutCommit(mergeInfo.getBase());
			File oursCheckout =
					mVdbRepository.checkoutCommit(mergeInfo.getOurs());
			File theirsCheckout =
					mVdbRepository.checkoutCommit(mergeInfo.getTheirs());

			mDb.execSQL("ATTACH DATABASE '"
					+ new File(baseCheckout, SQLITEDB).getAbsolutePath()
					+ "' AS base");
			mDb.execSQL("ATTACH DATABASE '"
					+ new File(oursCheckout, SQLITEDB).getAbsolutePath()
					+ "' AS ours");
			mDb.execSQL("ATTACH DATABASE '"
					+ new File(theirsCheckout, SQLITEDB).getAbsolutePath()
					+ "' AS theirs");
		}
	}

	/**
	 * Returns the database, opening if necessary. This operation
	 * grabs the read lock for the database.
	 * @return the database
	 */
	private synchronized SQLiteDatabase getDatabase() {
		openDatabase();
		try {
			if (mLock.readLock().tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
				return mDb;
			}
		} catch (InterruptedException e) {
			LOG.warn("Ignoring interupted exception: ", e);
		}
		throw new RuntimeException("Timeout waiting for the locked database.");
	}

	@Override
	public final synchronized SQLiteDatabase getReadOnlyDatabase()
			throws IOException {
		checkDeletedState();
		return getDatabase();
	}

	@Override
	public final synchronized SQLiteDatabase getReadWriteDatabase()
			throws IOException {
		checkDeletedState();
		checkReadOnly();
		return getDatabase();
	}

	@Override
	public final synchronized void releaseDatabase() {
		checkDeletedState();
		mLock.readLock().unlock();
	}

	/**
	 * Loads the merge information from the merge info file.
	 */
	private synchronized void loadMergeInfo() {
		File infoFile = new File(mDirectory, MERGEINFO);
		try {
			FileInputStream fis = new FileInputStream(infoFile);
			ObjectInputStream ois = new ObjectInputStream(fis);
			mMergeInfo = (MergeInfo) ois.readObject();
			ois.close();
		} catch (FileNotFoundException e) {
			mMergeInfo = null;
		} catch (IOException e) {
			throw new RuntimeException(
					"Error while reading MergeInformation from "
							+ infoFile, e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(
					"Error while reading MergeInformation from "
							+ infoFile, e);
		}
	}

	/**
	 * Saves the merge information to a a file.
	 */
	private synchronized void saveMergeInfo() {
		File infoFile = new File(mDirectory, MERGEINFO);
		if (mMergeInfo == null) {
			if (!infoFile.delete()) {
				LOG.warn("Error deleting: {}", infoFile);
			}
		} else {
			try {
				FileOutputStream fos = new FileOutputStream(infoFile);
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				oos.writeObject(mMergeInfo);
				oos.close();
			} catch (FileNotFoundException e) {
				throw new RuntimeException("Could not open "
						+ infoFile.getAbsolutePath()
						+ " for writing");
			} catch (IOException e) {
				throw new RuntimeException(
						"Error while reading MergeInformation from "
								+ infoFile, e);
			}
		}

	}

	@Override
	public final synchronized MergeInfo getMergeInfo() {
		checkDeletedState();
		if (mMergeInfo != null) {
			return mMergeInfo.clone();
		}
		return null;
	}

	@Override
	public final synchronized void doneMerge() {
		checkDeletedState();
		if (mMergeInfo == null) {
			throw new IllegalStateException("Branch was not in merge mode.");
		}
		mMergeInfo.setResolved();
		saveMergeInfo();
	}

	@Override
	public final synchronized void revert() throws IOException {
		checkDeletedState();
		try {
			Runtime.getRuntime().exec(new String[] {"rm", "-r",
					mDirectory.getAbsolutePath()}).waitFor();
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupt not allowed.");
		}
		mVdbRepository.checkoutBranch(mCheckoutName);
		mMergeInfo = null;
	}

	@Override
	public final synchronized void startMerge(final String theirSha1)
			throws MergeInProgressException, DirtyCheckoutException,
			IOException {
		checkDeletedState();
		if (mMergeInfo != null) {
			throw new MergeInProgressException();
		}

		// TODO: throw DirtyCheckoutException

		MergeInfo info;
		try {
			AnyObjectId theirCommit = mGitRepository.resolve(theirSha1);
			AnyObjectId ourCommit = mGitRepository.getRef(BRANCH_REF_PREFIX
					+ mCheckoutName).getObjectId();
			String baseCommit = mVdbRepository.getMergeBase(theirCommit,
					ourCommit).getId().getName();

			info = new MergeInfo(baseCommit,
					theirCommit.getName(), ourCommit.getName());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		// Only now save the merge state to the member variable
		// to prevent invalid merge
		// state in case part of the above operations fail.
		mMergeInfo = info;
		saveMergeInfo();
		attachMergeDatabases();
	}

	/**
	 * Check to ensure the checkout isn't flagged deleted.
	 */
	private void checkDeletedState() {
		if (mDeleted) {
			throw new IllegalStateException("This checkout was deleted.");
		}
	}

	/**
	 * Delete this checkout.
	 * @param path the path for the checkout
	 * @throws IOException if reading or writing fails.
	 */
	private void doDelete(final File path) throws IOException {
		if (path.isDirectory()) {
			for (File child : path.listFiles()) {
				doDelete(child);
			}
		}
		if (!path.delete()) {
			throw new IOException("Could not delete " + path);
		}
	}

	@Override
	public final void delete() {
		checkDeletedState();
		if (LOG.isDebugEnabled()) {
			LOG.debug("delete called for " + mCheckoutName);
		}

		try {
			if (!mLock.writeLock().tryLock(LOCK_TIMEOUT, TimeUnit.SECONDS)) {
				throw new RuntimeException(
						"Timeout waiting for exclusive lock on database.");
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		try {
			mDeleted = true;
			doDelete(mDirectory);
		} catch (IOException e) {
			throw new RuntimeException("Could not delete checkout.", e);
		} finally {
			mLock.writeLock().unlock();
			mVdbRepository.releaseCheckout(mCheckoutName);
		}
	}

	@Override
	public final String getSchema() throws IOException {
		LOG.debug("Getting schema from file: {} {}", mDirectory, SCHEMA_FILE);
		File schema = new File(mDirectory, SCHEMA_FILE);
		if (!schema.canRead()) {
			throw new RuntimeException("Unable to read schema file");
		}
		FileInputStream in = null;
		StringBuffer schemaBuffer = new StringBuffer();
		try {
			in = new FileInputStream(schema);
			int chr;
			while ((chr = in.read()) != -1) {
				schemaBuffer.append((char) chr);
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}
		LOG.debug("Read schema: {}", schemaBuffer);
		return schemaBuffer.toString();
	}

	private SQLiteDatabase getUpdateDatabase() throws IOException {
		if (mUpdateDb == null) {
			mUpdateDb = SQLiteDatabase.openDatabase(
					new File(mDirectory, "up_" + SQLITEDB).getAbsolutePath(),
					null /* cursor factory */,
					SQLiteDatabase.OPEN_READWRITE);
		}
		return mUpdateDb;
	}

	private void finishUpdate(SQLiteDatabase db, String newSchema)
			throws IOException {
		File upDbFile = new File(mUpdateDb.getPath());
		File currentDbFile =
				new File(mDirectory, SQLITEDB);

		if (!currentDbFile.delete()) {
			throw new RuntimeException("Unable to delete current.");
		}
		if (!upDbFile.renameTo(currentDbFile)) {
			throw new RuntimeException("Unable to move file in place.");
		}

		File schemaFile = new File(mDirectory, SCHEMA_FILE);
		if (!schemaFile.canWrite()) {
			throw new RuntimeException("Unable to write schema file");
		}

		FileOutputStream fos = new FileOutputStream(schemaFile);
		fos.write(newSchema.getBytes("utf8"));
		fos.close();
		db.close();
	}

	@Override
	public void updateDatabase(Schema newSchema) throws IOException {
		// Get the database
		SQLiteDatabase updateDb = getUpdateDatabase();

		// Build the initializer for the update db.
		Metadata updateMetadata =
				AvroContentProvider.makeMetadata(newSchema);
		DatabaseInitializer initializer =
				new DatabaseInitializer(newSchema.getNamespace(),
						updateMetadata, newSchema.toString());

		// Fill in the schema for the updated database.
		initializer.onCreate(updateDb);

		// Now attach the old database
		updateDb.execSQL("ATTACH DATABASE '"
				+ new File(mDirectory, SQLITEDB).getAbsolutePath()
				+ "' AS old");

		// Now copy all the data over.
		Metadata masterMetadata =
				AvroContentProvider.makeMetadata(Schema.parse(getSchema()));
		initializer.updateCopy(updateDb, masterMetadata);

		// Now finish
		finishUpdate(updateDb, newSchema.toString());

		updateDb.close();
	}

	/**
	 * Close the underlying database.
	 */
	public void close() {
		mDb.close();
	}
}
