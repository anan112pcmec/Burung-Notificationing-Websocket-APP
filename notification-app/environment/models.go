package environment

type Environment struct {
	CASS_ARCHIVE_KEYSPACE, CASS_ARCHIVE_PORT, CASS_ARCHIVE_PASS, CASS_ARCHIVE_USER string
	RDS_SESSION_HOST, RDS_SESSION_PORT                                             string
	RDS_SESSION_DB                                                                 int
}

func (*Environment) InitialArchiveDB(user, pass, port, keyspace string) *Environment {
	return &Environment{
		CASS_ARCHIVE_USER:     user,
		CASS_ARCHIVE_PASS:     pass,
		CASS_ARCHIVE_PORT:     port,
		CASS_ARCHIVE_KEYSPACE: keyspace,
	}
}

func (*Environment) InitialSessionCache(host, port string, db int) *Environment {
	return &Environment{
		RDS_SESSION_HOST: host,
		RDS_SESSION_PORT: port,
		RDS_SESSION_DB:   db,
	}
}
