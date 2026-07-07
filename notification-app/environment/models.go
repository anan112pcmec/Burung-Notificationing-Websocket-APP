package environment

type Environment struct {
	CASS_ARCHIVE_KEYSPACE, CASS_ARCHIVE_PORT, CASS_ARCHIVE_PASS, CASS_ARCHIVE_USER string
	RDS_SESSION_HOST, RDS_SESSION_PORT                                             string
	RDS_SESSION_DB                                                                 int
}

func (P *Environment) InitialArchiveDB(user, pass, port, keyspace string) *Environment {
	P.CASS_ARCHIVE_USER = user
	P.CASS_ARCHIVE_PASS = pass
	P.CASS_ARCHIVE_PORT = port
	P.CASS_ARCHIVE_KEYSPACE = keyspace
	return P // Mengembalikan pointer dirinya sendiri yang sudah terisi
}

func (P *Environment) InitialSessionCache(host, port string, db int) *Environment {
	P.RDS_SESSION_HOST = host
	P.RDS_SESSION_PORT = port
	P.RDS_SESSION_DB = db
	return P // Mengembalikan pointer dirinya sendiri yang sudah terisi
}
