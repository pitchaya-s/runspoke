# GPU resources

`runspk hardware` reports Linux CPU/RAM and NVIDIA GPU UUID, name, total/free memory and utilization via nvidia-smi. GPU memory and utilization are discovery-time observations, not enforceable budgets or a live utilization feed. Non-NVIDIA discovery is not implemented.

No physical GPU is advertised by default. Enable explicit UUIDs on a worker with `--gpu-ids GPU-UUID,...`. Do not advertise a desktop/display GPU or a GPU used outside runspk unless you have deliberately reserved it for this worker. Exclusive scheduling only governs jobs submitted to this scheduler; it cannot exclude unrelated host processes. The control plane rejects the same GPU identity advertised by two different worker IDs.

```bash
runspk-worker --id gpu-host --cpu 2 --mem-mib 4096 --allow-native --gpu-ids GPU-UUID
runspk submit --trusted --cpu 2 --mem 2G --gpu 1 --gpu-mem-mib 4096 -- python3 train.py
runspk submit --trusted --gpu 2 --gpu-ids GPU-A,GPU-B -- python3 train.py
```

GPU requests reserve whole identities exclusively. `--gpu-mem-mib` filters each GPU by **total capacity**, not free memory; it does not divide GPUs or enforce GPU memory isolation. `--gpu-ids` requests exact affinity and must name as many distinct identities as `--gpu`. Multi-GPU jobs must fit one worker. Reservations persist through uncertainty and release only on stopped completion or safe reconciliation.

Native jobs receive CUDA_VISIBLE_DEVICES containing allocated physical UUIDs. CPU jobs and simulated GPU jobs receive an empty value. This does not prevent trusted native code from accessing device files directly. Docker gets only allocated real UUIDs through `--gpus`; CPU/simulated jobs get NVIDIA_VISIBLE_DEVICES=void. No MIG, MPS, fractional GPU accounting or GPU memory enforcement is claimed.

For tests and CPU-only machines:

```bash
runspk-worker --id simulated --cpu 2 --mem-mib 256 --allow-native --simulate-gpus 2
runspk submit --trusted --gpu 2 --mem 64M -- /bin/echo simulated-allocation
```

Simulated GPUs have 4096 MiB each and identities `sim:WORKER:INDEX`. They test matching, exclusivity, multi-GPU allocation and affinity, and do not execute GPU kernels.
