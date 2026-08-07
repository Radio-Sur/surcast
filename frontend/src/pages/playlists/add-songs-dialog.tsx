import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { AddSongsDialog } from "@/components/add-songs-dialog";
import { useAddPlaylistSongs, usePlaylistSongsAll } from "@/hooks/use-playlists";
import { useSnackbar } from "@/providers/snackbar-provider";

export function AddSongsToPlaylistDialog({
  open,
  playlistId,
  onClose,
}: {
  open: boolean;
  playlistId: string;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const { showSnackbar } = useSnackbar();
  const queryClient = useQueryClient();
  const addSongs = useAddPlaylistSongs(playlistId);
  const { data: allData } = usePlaylistSongsAll(playlistId);
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
          queryClient.setQueryData(["playlists", playlistId, "songs", "all"], {
            songs: result,
            total: result.length,
            page: 1,
            per_page: result.length,
          });
          await queryClient.invalidateQueries({
            queryKey: ["playlists", playlistId, "songs"],
          });
          setIsRefetching(false);
          onClose();
          const added = result.length - beforeCount;
          if (added <= 0) return;
          if (added === 1) {
            const newSong = result.find((s) => !allSongs?.some((ps) => ps.song_id === s.song_id));
            showSnackbar(
              t("playlists:song_added", { title: newSong?.title ?? "", artist: newSong?.artist ?? "" }),
              "success",
            );
          } else {
            showSnackbar(t("playlists:songs_added", { count: added }), "success");
          }
        } catch (err) {
          setIsRefetching(false);
          console.error("Failed to add songs to playlist", err);
          showSnackbar("Failed to add songs to playlist", "error");
        }
      }}
      isPending={addSongs.isPending || isRefetching}
      existingSongIds={existingSongIds}
      existingArtistCounts={existingArtistCounts}
      title={t("playlists:add_songs_title")}
      searchPlaceholder={t("playlists:search_placeholder")}
      addLabel={(count) => t("playlists:add_songs_button", { count })}
      emptyLabel={t("playlists:add_songs_empty")}
    />
  );
}
