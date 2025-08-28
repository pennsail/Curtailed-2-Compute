
# battery.py
# Simple battery model for hourly simulation
from __future__ import annotations
from dataclasses import dataclass

@dataclass
class Battery:
    capacity_mwh: float                  # Energy capacity (MWh)
    max_charge_mw: float                 # Max charge power (MW)
    max_discharge_mw: float              # Max discharge power (MW)
    round_trip_efficiency: float = 0.92  # 0-1
    soc_mwh: float = 0.0                 # Initial state of charge (MWh)

    def __post_init__(self):
        if not (0.0 <= self.round_trip_efficiency <= 1.0):
            raise ValueError("round_trip_efficiency must be in [0,1].")
        # Split RTE into charge and discharge efficiencies (sqrt heuristic).
        self._eta_c = self.round_trip_efficiency ** 0.5
        self._eta_d = self.round_trip_efficiency ** 0.5

    @property
    def soc_pct(self) -> float:
        if self.capacity_mwh == 0:
            return 0.0
        return 100.0 * self.soc_mwh / self.capacity_mwh

    def available_for_discharge_mwh(self) -> float:
        # Energy that can be delivered to the load accounting for discharge efficiency.
        return self.soc_mwh * self._eta_d

    def headroom_for_charge_mwh(self) -> float:
        # Energy that can be *stored in SoC* (post-charge) this hour
        return max(self.capacity_mwh - self.soc_mwh, 0.0)

    def charge(self, request_mw: float, hours: float = 1.0) -> float:
        """Attempt to charge the battery.
        Args:
            request_mw: desired charging power (MW)
            hours: charging duration (h)
        Returns:
            actual_grid_energy_mwh: energy taken from source to charge (MWh)
        Notes:
            - Power limit: max_charge_mw
            - SoC limit: capacity_mwh
            - Charge efficiency: eta_c (SoC increases by input_energy * eta_c)
        """
        if request_mw <= 0 or hours <= 0:
            return 0.0
        power = min(request_mw, self.max_charge_mw)
        # Limit by SoC headroom: headroom = ΔSoC (post-charge), so input_energy = ΔSoC / eta_c
        max_input_energy_mwh = self.headroom_for_charge_mwh() / self._eta_c
        input_energy_mwh = min(power * hours, max_input_energy_mwh)
        delta_soc = input_energy_mwh * self._eta_c
        self.soc_mwh += delta_soc
        # Clamp
        if self.soc_mwh > self.capacity_mwh:
            self.soc_mwh = self.capacity_mwh
        return input_energy_mwh

    def discharge(self, request_mw: float, hours: float = 1.0) -> float:
        """Attempt to discharge the battery to meet load.
        Args:
            request_mw: desired discharge power at the load (MW)
            hours: duration (h)
        Returns:
            energy_delivered_mwh: energy delivered to the load (MWh)
        Notes:
            - Power limit: max_discharge_mw
            - SoC limited: energy available = soc_mwh * eta_d
            - SoC reduction = energy_delivered / eta_d
        """
        if request_mw <= 0 or hours <= 0:
            return 0.0
        power = min(request_mw, self.max_discharge_mw)
        need_energy_mwh = power * hours
        # Energy we can deliver given current SoC and efficiency
        max_deliverable_mwh = self.available_for_discharge_mwh()
        energy_delivered_mwh = min(need_energy_mwh, max_deliverable_mwh)
        # Reduce SoC by delivered/eta_d
        soc_drop = energy_delivered_mwh / self._eta_d
        self.soc_mwh = max(self.soc_mwh - soc_drop, 0.0)
        return energy_delivered_mwh
