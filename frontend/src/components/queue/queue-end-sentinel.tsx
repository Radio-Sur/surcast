import { useSortable } from "@dnd-kit/sortable";
import Box from "@mui/material/Box";
import { QUEUE_END } from "./";

export function QueueEndSentinel() {
  const { setNodeRef, isOver } = useSortable({ id: QUEUE_END });
  return (
    <Box
      ref={setNodeRef}
      sx={{
        height: 32,
        borderRadius: 2,
        bgcolor: isOver ? "action.selected" : "transparent",
        border: "2px dashed",
        borderColor: isOver ? "primary.main" : "divider",
        opacity: isOver ? 1 : 0.3,
      }}
    />
  );
}
