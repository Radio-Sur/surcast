import { DndContext } from "@dnd-kit/core";
import { SortableContext, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { describe, expect, it } from "vitest";
import { QueueEndSentinel } from "@/components/queue/queue-end-sentinel";
import { render } from "@/test/test-utils";

function renderWithDnd(ui: React.ReactElement) {
  return render(
    <DndContext>
      <SortableContext items={["__queue_end__"]} strategy={verticalListSortingStrategy}>
        {ui}
      </SortableContext>
    </DndContext>,
  );
}

describe("QueueEndSentinel", () => {
  it("renders drop zone", () => {
    const { container } = renderWithDnd(<QueueEndSentinel />);
    expect(container.firstChild).toBeInTheDocument();
  });
});
