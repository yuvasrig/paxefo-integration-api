package admin

import "net/http"

// notificationsLimit is how many alert occurrences the Notifications page lists.
const notificationsLimit = 200

// notificationsPage lists the alerts that are firing now and the ones that fired before (start, clear, how long).
func (c *Console) notificationsPage(w http.ResponseWriter, r *http.Request) {
	history, err := c.Store.ListAlertHistory(r.Context(), notificationsLimit)
	if err != nil {
		c.fail(w, r, "could not read the alert history", err)
		return
	}
	c.render(w, r, "notifications", view{Title: "Notifications", Nav: "notifications", AlertHistory: history})
}
