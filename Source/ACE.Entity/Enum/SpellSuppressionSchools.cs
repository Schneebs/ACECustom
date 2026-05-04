using System;

namespace ACE.Entity.Enum
{
    [Flags]
    public enum SpellSuppressionSchools
    {
        None = 0,
        WarMagic = 1 << 0,
        LifeMagic = 1 << 1,
        ItemEnchantment = 1 << 2,
        CreatureEnchantment = 1 << 3,
        VoidMagic = 1 << 4,

        All = WarMagic | LifeMagic | ItemEnchantment | CreatureEnchantment | VoidMagic,
    }
}
