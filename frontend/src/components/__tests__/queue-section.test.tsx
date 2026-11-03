import { within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { server } from "@/mocks/server";
import { QueueSection } from "@/pages/stations/queue-section";
import { render, screen, setupAuth, waitFor } from "@/test/test-utils";

beforeEach(() => {
  server.resetHandlers();
  setupAuth();
});

const baseQueueItem = {
  id: "q1",
  station_id: "s1",
  song_id: "s1",
  position: 0,
  title: "Test Song",
  artist: "Test Artist",
  album: "Test Album",
  duration: 200,
  has_cover: false,
  mime_type: "audio/mpeg",
  origin_playlist_id: null,
  playlist_name: null,
  is_auto_dj: false,
};

describe("QueueSection", () => {
  it("waits for bulk deletion and keeps failed selections for retry", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let started = false;
    server.use(
      http.delete("/api/stations/s1/queue/q1", async () => {
        started = true;
        await gate;
        return HttpResponse.json({ code: "INTERNAL" }, { status: 500 });
      }),
    );
    render(
      <QueueSection
        stationId="s1"
        queueSections={{ played: [], nowPlaying: null, upcoming: [baseQueueItem] }}
        streamStatus={null}
        playing={false}
        connected={false}
        elapsed={0}
        httpSkip={{ isPending: false, mutate: vi.fn() }}
        reorderQueue={{ isPending: false, mutate: vi.fn() }}
        removePlaylistFromQueue={{ isPending: false, mutate: vi.fn() }}
        handleRemoveFromQueue={vi.fn()}
        handleReAddToQueue={vi.fn()}
        setQueueAddOpen={vi.fn()}
      />,
    );
    const user = userEvent.setup();
    await user.click(await screen.findByRole("checkbox"));
    await user.click(screen.getByRole("button", { name: /delete selected/i }));
    const dialog = screen.getByRole("dialog");
    await user.click(within(dialog).getByRole("button", { name: /^delete$/i }));
    await waitFor(() => expect(started).toBe(true));
    expect(within(dialog).getByRole("button", { name: /^delete$/i })).toBeDisabled();
    release();
    await waitFor(() => expect(within(dialog).getByRole("button", { name: /^delete$/i })).toBeEnabled());
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });
  it("shows empty state when queue is empty", async () => {
    render(
      <QueueSection
        stationId="s1"
        queueSections={{ played: [], nowPlaying: null, upcoming: [] }}
        streamStatus={null}
        playing={false}
        connected={false}
        elapsed={0}
        httpSkip={{ isPending: false, mutate: vi.fn() }}
        reorderQueue={{ isPending: false, mutate: vi.fn() }}
        removePlaylistFromQueue={{ isPending: false, mutate: vi.fn() }}
        handleRemoveFromQueue={vi.fn()}
        handleReAddToQueue={vi.fn()}
        setQueueAddOpen={vi.fn()}
      />,
    );
    await waitFor(() => expect(screen.getByText(/queue is empty/i)).toBeInTheDocument(), { timeout: 5000 });
  });

  it("renders queue with items", async () => {
    render(
      <QueueSection
        stationId="s1"
        queueSections={{ played: [], nowPlaying: baseQueueItem, upcoming: [baseQueueItem] }}
        streamStatus={{
          playing: true,
          song_index: 0,
          total: 1,
          elapsed: 30,
          title: "Test Song",
          artist: "Test Artist",
          duration: 200,
        }}
        connected={true}
        playing={true}
        elapsed={30}
        httpSkip={{ isPending: false, mutate: vi.fn() }}
        reorderQueue={{ isPending: false, mutate: vi.fn() }}
        removePlaylistFromQueue={{ isPending: false, mutate: vi.fn() }}
        handleRemoveFromQueue={vi.fn()}
        handleReAddToQueue={vi.fn()}
        setQueueAddOpen={vi.fn()}
      />,
    );
    await waitFor(() => expect(screen.getAllByText("Test Song").length).toBeGreaterThanOrEqual(1), { timeout: 5000 });
    expect(screen.getByText(/play queue/i)).toBeInTheDocument();
  });

  it("renders add from library button", async () => {
    render(
      <QueueSection
        stationId="s1"
        queueSections={{ played: [], nowPlaying: baseQueueItem, upcoming: [baseQueueItem] }}
        streamStatus={null}
        playing={false}
        connected={false}
        elapsed={0}
        httpSkip={{ isPending: false, mutate: vi.fn() }}
        reorderQueue={{ isPending: false, mutate: vi.fn() }}
        removePlaylistFromQueue={{ isPending: false, mutate: vi.fn() }}
        handleRemoveFromQueue={vi.fn()}
        handleReAddToQueue={vi.fn()}
        setQueueAddOpen={vi.fn()}
      />,
    );
    await waitFor(() => expect(screen.getByText(/add from library/i)).toBeInTheDocument(), { timeout: 5000 });
  });
});
