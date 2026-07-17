"""Shared ID pools so all sources reference the same members and providers.

Import these everywhere instead of generating random IDs independently. This is
what makes the gold-layer joins actually match: a claim's member_id and
provider_id are guaranteed to exist in the members/providers tables.
"""

# Fixed pools - every generator draws from these same ranges.
MEMBER_IDS = [f"M{n}" for n in range(100000, 101000)]     # 1,000 members
PROVIDER_IDS = [f"P{n}" for n in range(1000, 1100)]       # 100 providers
