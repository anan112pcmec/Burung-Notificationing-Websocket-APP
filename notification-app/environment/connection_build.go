package environment

import (
	"fmt"
	"log"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/redis/go-redis/v9"

)

func (P *Environment) Connect() (error, *gocql.Session, *redis.Client) {
	ch := gocql.NewCluster(fmt.Sprintf("127.0.0.1:%s", P.CASS_ARCHIVE_PORT))
	ch.Keyspace = P.CASS_ARCHIVE_KEYSPACE
	ch.ProtoVersion = 4
	ch.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		MaxRetries:      8,                // 9 total percobaan (0s, 1s, 2s, 4s, 8s, 16s, 30s, 30s, 30s)
		InitialInterval: 1 * time.Second,  // Dimulai pada 1 detik
		MaxInterval:     30 * time.Second, // Membatasi pertumbuhan eksponensial hingga 30 detik
	}
	ch.Authenticator = gocql.PasswordAuthenticator{
		Username: P.CASS_ARCHIVE_USER,
		Password: P.CASS_ARCHIVE_PASS,
	}

	archive_db, err := ch.CreateSession()
	if err != nil {
		log.Fatal("gagal membuat session dengan cassandra", err)
	} else {
		fmt.Println("berhasil terhubung ke cassandra")
	}

	var redis_session = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", P.RDS_SESSION_HOST, P.RDS_SESSION_PORT),
		Password: "",
		DB:       P.RDS_SESSION_DB,
	})

	return nil, archive_db, redis_session
}
