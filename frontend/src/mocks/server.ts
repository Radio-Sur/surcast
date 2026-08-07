import { setupServer } from "msw/node";
import { apiKeysHandlers } from "./handlers/api-keys";
import { authHandlers } from "./handlers/auth";
import { icecastHandlers } from "./handlers/icecast";
import { listenersHandlers } from "./handlers/listeners";
import { playlistsHandlers } from "./handlers/playlists";
import { scheduleHandlers } from "./handlers/schedule";
import { songsHandlers } from "./handlers/songs";
import { stationsHandlers } from "./handlers/stations";
import { streamHandlers } from "./handlers/stream";
import { usersHandlers } from "./handlers/users";

export const server = setupServer(
  ...authHandlers,
  ...stationsHandlers,
  ...songsHandlers,
  ...playlistsHandlers,
  ...scheduleHandlers,
  ...usersHandlers,
  ...apiKeysHandlers,
  ...icecastHandlers,
  ...listenersHandlers,
  ...streamHandlers,
);
