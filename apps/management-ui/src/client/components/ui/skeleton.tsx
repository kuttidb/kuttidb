import { cn } from "cn"

/*
 * First-load skeleton: a static muted block that matches the final layout.
 * Deliberately not animated — no ornamental shimmer (design system, motion).
 */
function Skeleton({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="skeleton"
      className={cn("rounded-none bg-muted", className)}
      {...props}
    />
  )
}

export { Skeleton }
