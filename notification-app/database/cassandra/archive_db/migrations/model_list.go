package archive_migrations

import cass_models "burung-notificationing-app/notification-app/database/cassandra/models"

var model_list []interface{} = []interface{}{
	cass_models.NotificationPengguna{},
	cass_models.NotificationSeller{},
	cass_models.NotificationKurir{},
}
