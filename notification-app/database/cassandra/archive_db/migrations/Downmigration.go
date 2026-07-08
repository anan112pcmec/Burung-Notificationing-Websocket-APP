package archive_migrations

import (
	"context"
	"fmt"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	cass_models "burung-notificationing-app/notification-app/database/cassandra/models"
)

func DownRelation(ctx context.Context, session *gocql.Session) []error {
	var errs []error = []error{}

	for _, model := range model_list {
		fctx, cancel := context.WithTimeout(ctx, time.Second*6)

		if historicalModel, ok := model.(cass_models.TableName); ok {
			if err := cass_models.DropTable(fctx, session, historicalModel.TableNameArchive()); err != nil {
				errs = append(errs, err)
			}
		} else {
			fmt.Printf("Objek %T tidak mengimplementasikan archive_models.Method\n", model)
		}
		cancel()
	}

	return errs

}
