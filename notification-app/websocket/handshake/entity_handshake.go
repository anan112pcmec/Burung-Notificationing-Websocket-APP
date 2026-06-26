package entity_handshake

import (
	"log"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v3"

)

func HandshakePengguna(path string, dataActivePengguna map[int64]*websocket.Conn, app *fiber.App) {
	app.Get(path, websocket.New(func(c *websocket.Conn) {
		defer func() {
			for key, conn := range dataActivePengguna {
				if conn.Conn == c.Conn {
					delete(dataActivePengguna, key)
					log.Printf("[DISCONNECT] User '%v' (%s) terputus dan dihapus dari daftar online", key, conn.Conn)
				}

			}
		}()
	}))
}
