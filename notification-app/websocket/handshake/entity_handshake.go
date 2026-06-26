package entity_handshake_ws

import (
	"encoding/json"
	"fmt"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v3"

	connection_ws "burung-notificationing-app/notification-app/websocket/connection"
	error_ws "burung-notificationing-app/notification-app/websocket/error"
)

func HandshakePengguna(path string, dataActivePengguna *connection_ws.ActiveConnectionsEntity, app *fiber.App, logs_error *error_ws.ErrorLogs) {
	app.Get(path, websocket.New(func(c *websocket.Conn) {
		defer c.Close()

		var dataHandshake connection_ws.PenggunaBaseHandshake

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

		dataActivePengguna.AddConnection(dataHandshake.ID, &connection_ws.Koneksi{})

		suksesMsg := fmt.Sprintf("Handshake dengan pengguna %v %s Berhasil", dataHandshake.ID, dataHandshake.Username)
		_ = c.WriteMessage(websocket.TextMessage, []byte(suksesMsg))

	}))
}

// =========================================================================
// 2. HANDSHAKE SELLER
// =========================================================================
func HandshakeSeller(path string, dataActiveSeller *connection_ws.ActiveConnectionsEntity, app *fiber.App, logs_error *error_ws.ErrorLogs) {
	app.Get(path, websocket.New(func(c *websocket.Conn) {
		defer c.Close()

		var dataHandshake connection_ws.SellerBaseHandshake

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

		dataActiveSeller.AddConnection(int64(dataHandshake.ID), &connection_ws.Koneksi{})

		suksesMsg := fmt.Sprintf("Handshake dengan seller %v %s Berhasil", dataHandshake.ID, dataHandshake.Username)
		_ = c.WriteMessage(websocket.TextMessage, []byte(suksesMsg))
	}))
}

// =========================================================================
// 3. HANDSHAKE KURIR
// =========================================================================
func HandshakeKurir(path string, dataActiveKurir *connection_ws.ActiveConnectionsEntity, app *fiber.App, logs_error *error_ws.ErrorLogs) {
	app.Get(path, websocket.New(func(c *websocket.Conn) {
		defer c.Close()

		var dataHandshake connection_ws.KurirBaseHandshake

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

		dataActiveKurir.AddConnection(dataHandshake.ID, &connection_ws.Koneksi{})

		suksesMsg := fmt.Sprintf("Handshake dengan kurir %v %s Berhasil", dataHandshake.ID, dataHandshake.Username)
		_ = c.WriteMessage(websocket.TextMessage, []byte(suksesMsg))
	}))
}
