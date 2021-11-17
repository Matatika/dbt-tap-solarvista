[![Apache License](https://img.shields.io/badge/License-AGPLv3-blue.svg)](https://github.com/Matatika/dbt-tap-solarvista/blob/master/LICENSE) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=[%3E=0.20.x;%3C=1.0.0]&color=orange)
# Solarvista

This package models Solarvista Live data from [Matatika's Tap](https://github.com/Matatika/tap-solarvista).

The main focus of the package is to transform the revenue, project, work item, and engineer data into analytics for management, planning and performance insights.

This package along with the [Analyze Bundle](https://github.com/Matatika/analyze-solarvista) are designed intended to work together to provide instant insights on the [Matatika Platform](https://www.matatika.com).


## Models


| **model**                       | **description** |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| [dim_customer](models/base/dim_customer.sql)   | Customer name and reference information. |
| [dim_date](models/base/dim_date.sql)           | Date information including retail weeks of year. |
| [dim_equipment](models/base/dim_equipment.sql) | Equipment reference information. |
| [dim_project](models/base/dim_project.sql)     | Project information with [type-2 Slowly Changing Dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) available in `dim_project_snapshot`. |
| [dim_site](models/base/dim_site.sql)           | Site reference information. |
| [dim_territory](models/base/dim_territory.sql) | Territory reference information. |
| [dim_site](models/base/dim_site.sql)           | User reference information. |
| [dim_territory](models/base/dim_territory.sql) | Territory reference information. |
| [dim_user](models/base/dim_user.sql)           | User reference information with [type-2 Slowly Changing Dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) available in `dim_user_snapshot`.. |
| [fact_activity](models/base/fact_activity.sql)       | Activity facts. |
| [fact_appointment](models/base/fact_appointment.sql) | Appointment facts. |
| [fact_workitem](models/base/fact_workitem.sql)       | Work item facts including duration, travel time, and charges. |
| [fact_workitem_history](models/base/fact_workitem_history.sql) | History of work item stage changes (Assigned, Accepted, Travelling to, Working, Closed, etc). |
| [fact_workitem_stages](models/base/fact_workitem_stages.sql)       | Work item stage facts implemented as an [Accumulating Snapshot Fact Table](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/accumulating-snapshot-fact-table/). |
| [vw_daily_projects](models/base/vw_daily_projects.sql)   | Daily summary of projects for customers. |
| [vw_project_sla](models/base/vw_project_sla.sql)         | Summary of project service level performance. |
| [vw_workitem_stages](models/base/vw_workitem_stages.sql) | Summary of work item stages. |


## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions, or [read the dbt docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

Include in your `packages.yml`

```yaml
packages:
  - package: Matatika/dbt-tap-solarvista
    version: [">=0.1.0", "<1.0.0"]
```

### One time setup (after creating a python virtual environment)

    ```
    pip install dbt
    dbt deps
    ```

### development

    ```
    dbt test
    ```

## Database Support
This package has been tested on Postgres.

## Cloud hosting and SaaS
Deploy on the Matatika Platform within minutes. [www.matatika.com](https://www.matatika.com)

## Contributions

Additional contributions to this package are very welcome! Please create issues
or open PRs against `master`. Check out 
[this post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) 
on the best workflow for contributing to a package.

## Resources:
- Have questions, feedback, or need help? Please email us at support@matatika.com
- Find all of Matatika's pre-built dbt packages in our [dbt hub](https://hub.getdbt.com/Matatika/)
- Learn how to orchestrate [dbt transformations with Matatika](https://www.matatika.com/docs/getting-started/)
- Learn more about Matatika [in our docs](https://www.matatika.com/docs/introduction)
- Learn more about dbt [in the dbt docs](https://docs.getdbt.com/docs/introduction)

## License
You are granted use of this library under the [AGPLv3 License](https://github.com/Matatika/dbt-tap-solarvista/blob/master/LICENSE) terms.

This license grants you the freedom to use the software without any warranties, but requires you to open source your platform with the same terms if you distribute or host a service using any part of this library.  Contact us for a commercial embedded license to remove this restriction.

---

Copyright &copy; 2020 Matatika

