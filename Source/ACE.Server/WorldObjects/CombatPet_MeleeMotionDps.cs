using System;

using ACE.Entity.Enum.Properties;
using ACE.Server.Managers;

namespace ACE.Server.WorldObjects
{
    public partial class CombatPet
    {
        /// <summary>
        /// Scales per-hit melee damage so sustained melee DPS matches the summon weenie (template) motion + strike count,
        /// after visual/motion overrides from capture (or any MotionTableId change).
        /// </summary>
        private void ConfigureMeleeMotionDpsNormalization()
        {
            _meleeMotionDpsFactor = 1f;

            if (!ServerConfig.pet_melee_motion_dps_normalize.Value)
                return;

            GetCombatTable();
            if (CombatTable == null || Weenie == null)
                return;

            var baselineMotionId = MotionTableId;
            if (Weenie.PropertiesDID != null && Weenie.PropertiesDID.TryGetValue(PropertyDataId.MotionTable, out var fromWeenie))
                baselineMotionId = fromWeenie;

            var baselinePowerup = Weenie.PropertiesFloat != null && Weenie.PropertiesFloat.TryGetValue(PropertyFloat.PowerupTime, out var bpt)
                ? (double)bpt
                : 1.0;
            var currentPowerup = PowerupTime ?? 1.0f;

            var baselineDelayMean = (float)(baselinePowerup * 0.5);
            var currentDelayMean = (float)(currentPowerup * 0.5);

            var rateBase = EstimateMeleeDamageEventsPerSecond(baselineMotionId, baselineDelayMean);
            var rateCurrent = EstimateMeleeDamageEventsPerSecond(MotionTableId, currentDelayMean);

            if (rateBase <= 0 || rateCurrent <= 0 || rateCurrent <= float.Epsilon)
                return;

            var factor = rateBase / rateCurrent;
            var min = (float)ServerConfig.pet_melee_motion_dps_normalize_min.Value;
            var max = (float)ServerConfig.pet_melee_motion_dps_normalize_max.Value;
            if (min > 0 && max >= min)
                factor = Math.Clamp(factor, min, max);

            _meleeMotionDpsFactor = factor;
        }
    }
}
