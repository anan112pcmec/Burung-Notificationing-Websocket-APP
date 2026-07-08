package archive_migrations

import (
	"context"
	"fmt"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	cass_models "burung-notificationing-app/notification-app/database/cassandra/models"
)

func UpRelation(ctx context.Context, session *gocql.Session) []error {
	var errs []error = []error{}

	fmt.Println("uprelation jalan")

	for _, model := range model_list {
		// Tetap pakai timeout 6 detik per model biar ga gantung kalau lemot
		fctx, cancel := context.WithTimeout(ctx, time.Second*6)

		if historicalModel, ok := model.(cass_models.Method); ok {
			// session langsung dipakai dari parameter fungsi
			if err := historicalModel.CreateArchiveTable(fctx, session); err != nil {
				errs = append(errs, err)
			}

			fmt.Printf("Objek %T mengimplementasikan cass_models.Method\n", model)
		} else {
			fmt.Printf("Objek %T tidak mengimplementasikan cass_models.Method\n", model)
		}

		cancel() // Wajib di-cancel setelah selesai per iterasi biar ga leak memory
	}

	return errs
}
