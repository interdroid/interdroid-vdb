package interdroid.vdb.content.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import interdroid.vdb.Authority;
import interdroid.vdb.content.EntityUriBuilder;
import interdroid.vdb.content.VdbConfig.RepositoryConf;
import interdroid.vdb.content.VdbProviderRegistry;
import interdroid.vdb.content.metadata.DatabaseFieldType;
import interdroid.vdb.content.orm.DbEntity;
import interdroid.vdb.content.orm.DbField;
import interdroid.vdb.content.orm.ORMGenericContentProvider;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import android.content.ContentValues;
import android.content.Context;
import android.content.pm.ProviderInfo;
import android.database.Cursor;
import android.net.Uri;

public class AvroProviderRegistry extends ORMGenericContentProvider {
	private static final Logger logger = LoggerFactory.getLogger(AvroProviderRegistry.class);

	public static final String NAME = "schema_registry";
	public static final String NAMESPACE = "interdroid.vdb.content.avro";
	public static final String FULL_NAME = NAMESPACE + "." + NAME;

	public static final String KEY_SCHEMA = "schema";
	public static final String KEY_NAME = "name";
	public static final String KEY_NAMESPACE = "namespace";

	@DbEntity(name=NAME,
			itemContentType = "vnd.android.cursor.item/" + FULL_NAME,
			contentType = "vnd.android.cursor.dir/" + FULL_NAME)
	public static class RegistryConf {
		// Don't allow instantiation
		private RegistryConf() {}

		/**
		 * The default sort order for this table
		 */
		public static final String DEFAULT_SORT_ORDER = "modified DESC";

		public static final Uri CONTENT_URI =
			Uri.withAppendedPath(EntityUriBuilder.branchUri(Authority.VDB, AvroProviderRegistry.NAMESPACE, "master"), AvroProviderRegistry.NAME);

		@DbField(isID=true, dbType=DatabaseFieldType.INTEGER)
		public static final String _ID = "_id";

		@DbField(dbType=DatabaseFieldType.TEXT)
		public static final String NAME = KEY_NAME;

		@DbField(dbType=DatabaseFieldType.TEXT)
		public static final String NAMESPACE = KEY_NAMESPACE;

		@DbField(dbType=DatabaseFieldType.TEXT)
		public static final String SCHEMA = KEY_SCHEMA;

	}

	private Context mContext;

	public AvroProviderRegistry() {
		super(NAMESPACE, RegistryConf.class);
	}

	public static final Uri URI = Uri.withAppendedPath(EntityUriBuilder.branchUri(Authority.VDB,
			NAMESPACE, "master"), NAME);

	public List<RepositoryConf> getAllRepositories() {
		Cursor c = null;
		ArrayList<RepositoryConf> result = new ArrayList<RepositoryConf>();
		try {
			c = query(URI, new String[]{KEY_NAMESPACE, KEY_SCHEMA}, null, null, null);
			if (c != null) {
				int namespaceIndex = c.getColumnIndex(KEY_NAMESPACE);
				int schemaIndex = c.getColumnIndex(KEY_SCHEMA);
				while (c.moveToNext()) {
					result.add(new RepositoryConf(c.getString(namespaceIndex), c.getString(schemaIndex)));
				}
			}
		} finally {
			if (c != null) {
				try {
					c.close();
				} catch (Exception e) {
					logger.warn("Exception while closing cursor: ", e);
				}
			}
		}

		return result;
	}

	@Override
	public void attachInfo(Context context, ProviderInfo info) {
		super.attachInfo(context, info);
		mContext = context;
	}

	@Override
	public Uri insert(Uri uri, ContentValues userValues)
	{
		Uri result = super.insert(uri, userValues);

		VdbProviderRegistry registry;
		try {
			registry = new VdbProviderRegistry(mContext);
			registry.registerRepository(new RepositoryConf(userValues.getAsString(KEY_NAMESPACE), userValues.getAsString(KEY_SCHEMA)));
		} catch (IOException e) {
			throw new RuntimeException("Unable to build registry: ", e);
		}

		return result;
	}

	public static void registerSchema(Context context, Schema schema) throws IOException {
		// Have we already registered?
		Cursor c = null;
		try {
			logger.debug("Checking for registration of {}", schema.getName());
			logger.debug("Querying against URI: {}", URI);
			c = context.getContentResolver().query(URI,
					new String[] {KEY_SCHEMA},
					KEY_NAME +" = ?", new String[] {schema.getName()}, null);
			logger.debug("Got cursor: {}", c);
			if (c != null) {
				if (c.getCount() == 0) {
					logger.debug("Not already registered.");
					ContentValues values = new ContentValues();
					values.put(KEY_SCHEMA, schema.toString());
					values.put(KEY_NAME, schema.getName());
					values.put(KEY_NAMESPACE, schema.getNamespace());
					context.getContentResolver().insert(URI, values);
					// Register this schema as a provider
					new VdbProviderRegistry(context).registerRepository(
							new RepositoryConf(schema.getNamespace(), schema.toString()));
				} else {
					// Do we need to update the schema then?
					logger.debug("Checking if we need to update.");
					c.moveToFirst();
					Schema currentSchema = Schema.parse(c.getString(c.getColumnIndex(KEY_SCHEMA)));
					if (! schema.equals(currentSchema)) {
						logger.debug("Update required.");

						migrateDb(context, currentSchema, schema);

						ContentValues values = new ContentValues();
						values.put(KEY_SCHEMA, schema.toString());
						context.getContentResolver().update(URI,
								values, KEY_NAME +" = ?", new String[]{AvroProviderRegistry.KEY_NAME});
					}
				}
			} else {
				logger.error("Unexpected error registering schema");
				throw new RuntimeException("Unable to query Schema Registry!");
			}
		} finally {
			if (c != null) {
				c.close();
			}
		}
		logger.debug("Schema registration complete.");
	}

	private static void migrateDb(Context context, Schema currentSchema,
			Schema schema) {
		Cursor c = null;
		try {
			c = context.getContentResolver().query(
					EntityUriBuilder.branchUri(Authority.VDB, currentSchema.getNamespace(),
							"master/" + currentSchema.getName()), new String[] {"_id"}, null, null, null);
		} finally {
			try {
				if (c == null) {
					c.close();
				}
			} catch (Exception e) {
				logger.error("Caught excption closing cursor", e);
			}
		}
	}

	public static Schema getSchema(Context context, Uri uri) {
		Cursor c = null;
		Schema schema = null;
		try {
			// We expect to deal with internal paths
			if (!Authority.VDB.equals(uri.getAuthority())) {
				logger.debug("Mapping to native: {}", uri);
				uri = EntityUriBuilder.toInternal(uri);
			}
			logger.debug("Querying for schema for: {} {}", uri, uri.getPathSegments().get(0));
			c = context.getContentResolver().query(URI, new String[]{KEY_SCHEMA}, KEY_NAMESPACE + "=?", new String[] {uri.getPathSegments().get(0)}, null);
			if (c != null && c.moveToFirst()) {
				int schemaIndex = c.getColumnIndex(KEY_SCHEMA);
				String schemaString = c.getString(schemaIndex);
				logger.debug("Got schema: {}", schemaString);
				schema = Schema.parse(schemaString);
			} else {
				logger.error("Schema not found.");
			}
		} finally {
			if (c != null) {
				try {
					c.close();
				} catch (Exception e) {
					logger.warn("Exception while closing cursor: ", e);
				}
			}
		}
		return schema;
	}
}
