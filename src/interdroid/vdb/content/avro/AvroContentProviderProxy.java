package interdroid.vdb.content.avro;

import java.io.IOException;

import interdroid.vdb.Actions;
import interdroid.vdb.Authority;
import interdroid.vdb.content.ContentChangeHandler;
import interdroid.vdb.content.EntityUriMatcher;
import interdroid.vdb.content.EntityUriMatcher.UriMatch;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import android.content.ComponentName;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.pm.ProviderInfo;
import android.database.CrossProcessCursor;
import android.database.Cursor;
import android.database.CursorWindow;
import android.database.CursorWrapper;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.os.IBinder;
import android.os.Messenger;
import android.os.RemoteException;

public class AvroContentProviderProxy extends ContentProvider {

	private static final Logger logger = LoggerFactory.getLogger(AvroContentProviderProxy.class);

	// TODO: Need to proxy content change delete update etc notifications.
	// TODO: Need to check for installation of vdb-ui and make sure we are all set.

	protected final Schema schema_;

	public AvroContentProviderProxy(String schema) {
		this(Schema.parse(schema));
	}

	public AvroContentProviderProxy(Schema schema) {
		logger.debug("Constructing provider proxy.");
		schema_ = schema;
	}

	private Uri remapUri(Uri uri) {
		Uri.Builder builder = new Uri.Builder();
		builder.scheme(uri.getScheme());
		builder.authority(Authority.VDB);
		builder.path(uri.getAuthority() + uri.getPath());
		builder.query(uri.getQuery());
		Uri built = builder.build();
		logger.debug("remapped: {} to {}", uri, built);
		return built;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		return getContext().getContentResolver().delete(remapUri(uri), selection, selectionArgs);
	}

	@Override
	public String getType(Uri uri) {
		return getContext().getContentResolver().getType(remapUri(uri));
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		final UriMatch result = EntityUriMatcher.getMatch(uri);
		ContentChangeHandler handler = ContentChangeHandler.getHandler(result.authority, result.entityName);
		if (handler != null) {
			handler.preInsertHook(values);
		}
		Context context = getContext();
		ContentResolver resolver = context.getContentResolver();
		Uri mappedUri = remapUri(uri);
		logger.debug("Inserting into:" + mappedUri + " values: " + values);

		return resolver.insert(mappedUri, values);
	}

	@Override
	public void attachInfo(Context context, ProviderInfo info) {
		super.attachInfo(context, info);

		// Make sure we are registered.
		logger.debug("attachInfo");
		logger.debug("Registering schema: {}", schema_.getName());

		try {
			AvroSchemaRegistrationHandler.registerSchema(context, schema_);
		} catch (IOException e) {
			logger.error("Caught IOException while registering.", e);
		}

	}

	private class CrossProcessCursorWrapper extends CursorWrapper
		implements CrossProcessCursor {

		public CrossProcessCursorWrapper(Cursor cursor) {
			super(cursor);
		}

		@Override
		public CursorWindow getWindow() {
			return null;
		}

		@Override
		public void fillWindow(int position, CursorWindow window) {
			if (position < 0 || position > getCount()) {
				return;
			}
			window.acquireReference();
			try {
				moveToPosition(position - 1);
				window.clear();
				window.setStartPosition(position);
				int columnNum = getColumnCount();
				window.setNumColumns(columnNum);
				while (moveToNext() && window.allocRow()) {
					for (int i = 0; i < columnNum; i++) {
						String field = getString(i);
						if (field != null) {
							if (!window.putString(field, getPosition(), i)) {
								window.freeLastRow();
								break;
							}
						} else {
							if (!window.putNull(getPosition(), i)) {
								window.freeLastRow();
								break;
							}
						}
					}
				}
			} catch (IllegalStateException e) {
				logger.error("Exception with wrapped cursor", e);
			} finally {
				window.releaseReference();
			}
		}

		@Override
		public boolean onMove(int oldPosition, int newPosition) {
			return true;
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		return new CrossProcessCursorWrapper(getContext().getContentResolver().query(remapUri(uri), projection, selection, selectionArgs, sortOrder));
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return getContext().getContentResolver().update(remapUri(uri), values, selection, selectionArgs);
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		return false;
	}

}
