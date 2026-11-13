FROM busybox:1.36.1 AS jdbc-fetch

ADD --checksum=sha256:188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
  /tmp/postgresql-42.7.4.jar
RUN chmod 0644 /tmp/postgresql-42.7.4.jar \
    && test -s /tmp/postgresql-42.7.4.jar

# The pinned upstream fixture is the application base.  Its official runtime
# uses org.apache.iceberg.rest.RESTCatalogServer; the default single-archive
# launch does not include an externally copied JDBC jar, so the command below
# uses an explicit classpath.
FROM apache/iceberg-rest-fixture:1.10.0

COPY --from=jdbc-fetch /tmp/postgresql-42.7.4.jar /usr/lib/iceberg-rest/postgresql-42.7.4.jar

ENV POSTGRESQL_JDBC_JAR=/usr/lib/iceberg-rest/postgresql-42.7.4.jar

CMD ["java", "-cp", "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql-42.7.4.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
