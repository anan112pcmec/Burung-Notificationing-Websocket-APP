package entity_handshake_ws

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"

	"burung-notificationing-app/notification-app/identity/identity_kurir"
	"burung-notificationing-app/notification-app/identity/identity_pengguna"
	"burung-notificationing-app/notification-app/identity/identity_seller"
	connection_models_ws "burung-notificationing-app/notification-app/websocket/connection"
	error_ws "burung-notificationing-app/notification-app/websocket/error"
)

func HandshakePengguna(path string, dataActivePengguna *connection_models_ws.ActiveConnectionsEntity, app *fiber.App, logs_error *error_ws.ErrorLogs, session *redis.Client) {
	app.Get(path, websocket.New(func(c *websocket.Conn) {
		defer c.Close()
		konteks, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		var dataHandshake identity_pengguna.IdentityPengguna

		_, msg, err := c.ReadMessage()
		if err != nil {
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			return
		}

		if err := json.Unmarshal(msg, &dataHandshake); err != nil {
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			return
		}

		if valid := dataHandshake.Validating(konteks, session); !valid {
			_ = c.WriteMessage(websocket.TextMessage, []byte("Anda tidak memiliki sesi yang valid"))
			return
		}

		dataActivePengguna.AddConnection(dataHandshake.ID, &connection_models_ws.Koneksi{})

		suksesMsg := fmt.Sprintf("Handshake dengan pengguna %v %s Berhasil", dataHandshake.ID, dataHandshake.Username)
		_ = c.WriteMessage(websocket.TextMessage, []byte(suksesMsg))

	}))
}

// =========================================================================
// 2. HANDSHAKE SELLER
// =========================================================================
func HandshakeSeller(path string, dataActiveSeller *connection_models_ws.ActiveConnectionsEntity, app *fiber.App, logs_error *error_ws.ErrorLogs, session *redis.Client) {
	app.Get(path, websocket.New(func(c *websocket.Conn) {
		defer c.Close()
		konteks, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		var dataHandshake identity_seller.IdentitySeller

		_, msg, err := c.ReadMessage()
		if err != nil {
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			return
		}

		if err := json.Unmarshal(msg, &dataHandshake); err != nil {
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			return
		}

		if valid := dataHandshake.Validating(konteks, session); !valid {
			_ = c.WriteMessage(websocket.TextMessage, []byte("Anda tidak memiliki sesi yang valid"))
			return
		}

		dataActiveSeller.AddConnection(int64(dataHandshake.IdSeller), &connection_models_ws.Koneksi{})

		suksesMsg := fmt.Sprintf("Handshake dengan seller %v %s Berhasil", dataHandshake.IdSeller, dataHandshake.Username)
		_ = c.WriteMessage(websocket.TextMessage, []byte(suksesMsg))
	}))
}

// =========================================================================
// 3. HANDSHAKE KURIR
// =========================================================================
func HandshakeKurir(path string, dataActiveKurir *connection_models_ws.ActiveConnectionsEntity, app *fiber.App, logs_error *error_ws.ErrorLogs, session *redis.Client) {
	app.Get(path, websocket.New(func(c *websocket.Conn) {
		defer c.Close()
		konteks, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		var dataHandshake identity_kurir.IdentitasKurir

		_, msg, err := c.ReadMessage()
		if err != nil {
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			return
		}

		if err := json.Unmarshal(msg, &dataHandshake); err != nil {
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			return
		}

		if valid := dataHandshake.Validating(konteks, session); !valid {
			_ = c.WriteMessage(websocket.TextMessage, []byte("Anda tidak memiliki sesi yang valid"))
			return
		}

		dataActiveKurir.AddConnection(dataHandshake.IdKurir, &connection_models_ws.Koneksi{})

		suksesMsg := fmt.Sprintf("Handshake dengan kurir %v %s Berhasil", dataHandshake.IdKurir, dataHandshake.UsernameKurir)
		_ = c.WriteMessage(websocket.TextMessage, []byte(suksesMsg))
	}))
}
