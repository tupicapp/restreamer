package cmd

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/tupicapp/restreamer/api"
)

func NewServerCommand() *cobra.Command {
	var addr string

	cmd := &cobra.Command{
		Use:   "server",
		Short: "Run the Irajstreamer API server",
		RunE: func(cmd *cobra.Command, args []string) error {
			srv := api.NewServer(addr)
			log.Printf("irajstreamer api starting on %s", addr)

			errCh := make(chan error, 1)
			go func() {
				errCh <- srv.ListenAndServe()
			}()

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			select {
			case <-ctx.Done():
				log.Printf("irajstreamer api shutting down")
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return srv.Shutdown(shutdownCtx)
			case err := <-errCh:
				if err == nil || err == http.ErrServerClosed {
					log.Printf("irajstreamer api stopped")
					return nil
				}
				log.Printf("irajstreamer api error: %v", err)
				return err
			}
		},
	}

	cmd.Flags().StringVar(&addr, "addr", ":8080", "API listen address")
	return cmd
}
