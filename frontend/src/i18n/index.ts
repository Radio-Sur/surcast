import i18n from "i18next";
import LanguageDetector from "i18next-browser-languagedetector";
import { initReactI18next } from "react-i18next";
import enApiKeys from "./en/api-keys.json";
import enAuth from "./en/auth.json";
import enCommon from "./en/common.json";
import enDashboard from "./en/dashboard.json";
import enErrors from "./en/errors.json";
import enNav from "./en/nav.json";
import enPlaylists from "./en/playlists.json";
import enSchedule from "./en/schedule.json";
import enSettings from "./en/settings.json";
import enSongs from "./en/songs.json";
import enStations from "./en/stations.json";
import enUsers from "./en/users.json";
import plApiKeys from "./pl/api-keys.json";
import plAuth from "./pl/auth.json";
import plCommon from "./pl/common.json";
import plDashboard from "./pl/dashboard.json";
import plErrors from "./pl/errors.json";
import plNav from "./pl/nav.json";
import plPlaylists from "./pl/playlists.json";
import plSchedule from "./pl/schedule.json";
import plSettings from "./pl/settings.json";
import plSongs from "./pl/songs.json";
import plStations from "./pl/stations.json";
import plUsers from "./pl/users.json";

const resources = {
  en: {
    common: enCommon,
    nav: enNav,
    auth: enAuth,
    songs: enSongs,
    stations: enStations,
    schedule: enSchedule,
    settings: enSettings,
    playlists: enPlaylists,
    "api-keys": enApiKeys,
    users: enUsers,
    dashboard: enDashboard,
    errors: enErrors,
  },
  pl: {
    common: plCommon,
    nav: plNav,
    auth: plAuth,
    songs: plSongs,
    stations: plStations,
    schedule: plSchedule,
    settings: plSettings,
    playlists: plPlaylists,
    "api-keys": plApiKeys,
    users: plUsers,
    dashboard: plDashboard,
    errors: plErrors,
  },
};

i18n
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    resources,
    fallbackLng: "en",
    defaultNS: "common",
    interpolation: {
      escapeValue: false,
    },
    returnNull: false,
  });

export default i18n;
