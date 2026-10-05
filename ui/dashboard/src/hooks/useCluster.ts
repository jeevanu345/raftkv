import { useQuery } from "@tanstack/react-query";
import { api } from "../lib/api";

export function useCluster() {
  return useQuery({
    queryKey: ["cluster"],
    queryFn: api.getCluster,
    refetchInterval: 2_000
  });
}
