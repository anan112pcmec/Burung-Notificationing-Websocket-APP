package prevision

type Previsioning struct {
	CASS_ARCHIVE_KEYSPACE, CASS_ARCHIVE_PORT, CASS_ARCHIVE_PASS, CASS_ARCHIVE_USER string
	RDS_SESSION_HOST, RDS_SESSION_PORT, RDS_SESSION_DB                             string
}

func (*Previsioning) InitialArchiveDB(user, pass, port, keyspace string) *Previsioning {
	return &Previsioning{
		CASS_ARCHIVE_USER:     user,
		CASS_ARCHIVE_PASS:     pass,
		CASS_ARCHIVE_PORT:     port,
		CASS_ARCHIVE_KEYSPACE: keyspace,
	}
}

func (*Previsioning) InitialSessionCache(host, port, db string) *Previsioning {
	return &Previsioning{
		RDS_SESSION_HOST: host,
		RDS_SESSION_PORT: port,
		RDS_SESSION_DB:   db,
	}
}
