import { expect, it, vi } from "vitest";
import { AutoDJFormFields } from "@/components/schedule/auto-dj-form-fields";
import { render, screen } from "@/test/test-utils";

it.each([null, false, true])("defaults artist-repeat avoidance off while honoring the saved value %s", (saved) => {
  render(
    <AutoDJFormFields
      autoDjMode="random"
      autoDjAvoidRepeat={saved}
      autoDjMinGap={null}
      autoDjSongsAhead={null}
      readOnly={false}
      onChange={vi.fn()}
    />,
  );
  const control = screen.getByLabelText("Avoid artist repeat");
  if (saved === true) expect(control).toBeChecked();
  else expect(control).not.toBeChecked();
});
