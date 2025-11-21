using System.Collections.Generic;

using ACE.Database.Models.Shard;

namespace ACE.Database.Entity
{
    public class PossessedBiotas
    {
        public List<Biota> Inventory { get; } = new List<Biota>();

        public List<Biota> WieldedItems { get; } = new List<Biota>();

        /// <summary>
        /// Indicates if item loading failed completely. If true, player should not be allowed to log in.
        /// </summary>
        public bool LoadFailed { get; set; }

        /// <summary>
        /// Error message describing why loading failed
        /// </summary>
        public string LoadFailureReason { get; set; }

        public PossessedBiotas(ICollection<Biota> inventory, ICollection<Biota> wieldedItems)
        {
            Inventory.AddRange(inventory);

            WieldedItems.AddRange(wieldedItems);
        }
    }
}
