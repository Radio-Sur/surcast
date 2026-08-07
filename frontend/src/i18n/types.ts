import type enApiKeys from "./en/api-keys.json";
import type enAuth from "./en/auth.json";
import type enCommon from "./en/common.json";
import type enDashboard from "./en/dashboard.json";
import type enErrors from "./en/errors.json";
import type enNav from "./en/nav.json";
import type enPlaylists from "./en/playlists.json";
import type enSchedule from "./en/schedule.json";
import type enSettings from "./en/settings.json";
import type enSongs from "./en/songs.json";
import type enStations from "./en/stations.json";
import type enUsers from "./en/users.json";

export type TranslationResources = {
  common: typeof enCommon;
  nav: typeof enNav;
  auth: typeof enAuth;
  songs: typeof enSongs;
  stations: typeof enStations;
  schedule: typeof enSchedule;
  settings: typeof enSettings;
  playlists: typeof enPlaylists;
  "api-keys": typeof enApiKeys;
  users: typeof enUsers;
  dashboard: typeof enDashboard;
  errors: typeof enErrors;
};

declare module "i18next" {
  interface CustomTypeOptions {
    resources: TranslationResources;
    returnNull: false;
  }
}
