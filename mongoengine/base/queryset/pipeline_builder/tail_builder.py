class TailBuilder:
    """Builds aggregation stages that must always run last."""

    @staticmethod
    def build(queryset):
        pipeline = []

        lf = queryset._loaded_fields
        if lf:
            proj = lf.as_dict()
            if "_id" not in proj:
                proj["_id"] = 1
            pipeline.append({"$project": proj})

        if queryset._ordering:
            pipeline.append({"$sort": dict(queryset._ordering)})

        if queryset._skip:
            pipeline.append({"$skip": queryset._skip})

        if queryset._limit is not None:
            pipeline.append({"$limit": queryset._limit})

        return pipeline
