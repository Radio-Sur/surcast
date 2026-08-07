import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { AddSongsDialog } from "@/components/add-songs-dialog";
import { useAddStationSongs, useStationSongsAll } from "@/hooks/use-station-library";
import { useSnackbar } from "@/providers/snackbar-provider";

export function AddSongsToStationDialog({
  open,
  stationId,
  onClose,
}: {
  open: boolean;
  stationId: string;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const { showSnackbar } = useSnackbar();
  const queryClient = useQueryClient();
  const addSongs = useAddStationSongs(stationId);
  const { data: allData } = useStationSongsAll(stationId);
  const allSongs = allData?.songs;
  const [isRefetching, setIsRefetching] = useState(false);

  const existingSongIds = new Set(allSongs?.map((s) => s.song_id) ?? []);
  const existingArtistCounts: Record<string, number> = {};
  for (const s of allSongs ?? []) {
    existingArtistCounts[s.artist] = (existingArtistCounts[s.artist] ?? 0) + 1;
  }

  return (
    <AddSongsDialog
      open={open}
      onClose={onClose}
      onAdd={async (selections) => {
        try {
          const beforeCount = allSongs?.length ?? 0;
          const result = await addSongs.mutateAsync(selections);
          setIsRefetching(true);
          queryClient.setQueryData(["station-songs", stationId, "all"], {
            songs: result,
            total: result.length,
            page: 1,
            per_page: result.length,
          });
          await queryClient.invalidateQueries({
            queryKey: ["station-songs", stationId],
          });
          setIsRefetching(false);
          onClose();
          const added = result.length - beforeCount;
          if (added <= 0) return;
          if (added === 1) {
            const newSong = result.find((s) => !allSongs?.some((ls) => ls.song_id === s.song_id));
            showSnackbar(
              t("stations:song_added", { title: newSong?.title ?? "", artist: newSong?.artist ?? "" }),
              "success",
            );
          } else {
            showSnackbar(t("stations:songs_added", { count: added }), "success");
          }
        } catch (err) {
          setIsRefetching(false);
          console.error("Failed to add songs to station library", err);
          showSnackbar("Failed to add songs to station library", "error");
        }
      }}
      isPending={addSongs.isPending || isRefetching}
      existingSongIds={existingSongIds}
      existingArtistCounts={existingArtistCounts}
      title={t("stations:library_dialog_title")}
      searchPlaceholder={t("songs:search_placeholder")}
      addLabel={(count) => t("stations:library_dialog_add", { count })}
      emptyLabel={t("stations:library_dialog_empty")}
    />
  );
}
