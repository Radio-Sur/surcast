import { HttpResponse, http } from "msw";
import { expect, it, vi } from "vitest";
import { CreateUserDialog } from "@/components/create-user-dialog";
import { server } from "@/mocks/server";
import { render, screen, setupAuth, userEvent, waitFor } from "@/test/test-utils";

it("creates an account through the admin endpoint and clears the dialog on success", async () => {
  setupAuth();
  const submitted = vi.fn();
  server.use(
    http.post("/api/users", async ({ request }) => {
      const data = await request.json();
      submitted(data);
      return HttpResponse.json({ id: "new", ...(data as object) }, { status: 201 });
    }),
  );
  const close = vi.fn();
  const user = userEvent.setup();
  render(<CreateUserDialog onClose={close} />);
  await user.type(screen.getByLabelText(/username/i), "new-admin");
  await user.type(screen.getByLabelText(/password/i), "safe-password");
  await user.click(screen.getByRole("button", { name: "Create" }));
  await waitFor(() => expect(close).toHaveBeenCalledOnce());
  expect(submitted).toHaveBeenCalledWith({
    username: "new-admin",
    password: "safe-password",
    role: "admin",
  });
});
