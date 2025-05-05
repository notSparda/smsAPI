const express = require('express');
const cors = require('cors');
const pool = require('./db');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

app.get('/', async (req, res) => {
    try {
        res.json("WELCOME TO STUDENT API");
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
})

app.get("/job_history", async (req, res) => {
    try {
        const result = await pool.query(
            "select jh.*,e.first_name,e.last_name,c.country_name,j.job_title from job_history jh inner join employees e on e.employee_id = jh.employee_id inner join jobs j on j.job_id = jh.job_id inner join departments d on d.department_id = e.department_id inner join locations l on l.location_id = d.location_id inner join countries c on c.country_id = l.country_id ;");
        if (!result) return res.status(401).json({ msg: "incorrect query" })
        return res.status(200).json({ data: result.rows })
    } catch (err) {
        res.status(500).json({ Error: err.message })
    }
})


app.get("/regions_countries_locations", async (req, res) => {
    try {
        const result = await pool.query(
            "select r.*, c.country_name, l.city, l.location_id from regions r inner join countries c ON c.region_id = r.region_id inner join locations l ON l.country_id = c.country_id;");
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});
app.get("/countries_regions_locations", async (req, res) => {
    try {
        const result = await pool.query(
            "select c.*, r.region_name, l.city, l.location_id from countries c inner join regions r ON c.region_id = r.region_id inner join locations l ON l.country_id = c.country_id;");

        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/locations_with_countries_regions", async (req, res) => {
    try {
        const result = await pool.query(
            "select l.city, l.street_address, c.country_name, r.region_name from locations l inner join countries c on l.country_id = c.country_id inner join regions r on c.region_id = r.region_id;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});
app.get("/departments_with_employees_locations", async (req, res) => {
    try {
        const result = await pool.query(
            "select d.department_name, e.first_name, e.last_name, l.city, l.street_address from departments d inner join employees e on d.department_id = e.department_id inner join locations l on d.location_id = l.location_id;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});



app.get("/employees_with_departments_locations_countries", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.first_name, e.last_name, d.department_name, l.city, l.street_address, c.country_name from employees e inner join departments d on e.department_id = d.department_id inner join locations l on d.location_id = l.location_id inner join countries c on l.country_id = c.country_id;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/employees_with_managers_departments_locations", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.first_name as employee_first_name, e.last_name as employee_last_name, m.first_name as manager_first_name, m.last_name as manager_last_name, d.department_name, l.city from employees e inner join employees m on e.manager_id = m.employee_id inner join departments d on e.department_id = d.department_id inner join locations l on d.location_id = l.location_id;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});



app.get("/employees_job_dept_location", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.first_name, e.last_name, j.job_title, d.department_name, l.city from employees e inner join jobs j on e.job_id = j.job_id inner join departments d on e.department_id = d.department_id inner join locations l on d.location_id = l.location_id;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/employees_job_dept_manager", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.first_name, e.last_name, j.job_title, d.department_name, m.first_name as manager_first, m.last_name as manager_last from employees e inner join jobs j on e.job_id = j.job_id inner join departments d on e.department_id = d.department_id left join employees m on e.manager_id = m.employee_id;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/employees_full_details", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.first_name, e.last_name, j.job_title, d.department_name, m.first_name as manager_first, m.last_name as manager_last, l.city from employees e inner join jobs j on e.job_id = j.job_id inner join departments d on e.department_id = d.department_id left join employees m on e.manager_id = m.employee_id inner join locations l on d.location_id = l.location_id;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/countries_in_region1", async (req, res) => {
    try {
        const result = await pool.query(
            "select country_name from countries where region_id = 1;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/departments_in_cities_n", async (req, res) => {
    try {
        const result = await pool.query(
            "select d.department_name, l.city from departments d inner join locations l on d.location_id = l.location_id where l.city ilike 'n%';"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/employees_under_high_commission_managers", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.first_name, e.last_name, d.department_name from employees e inner join departments d on e.department_id = d.department_id inner join employees m on d.manager_id = m.employee_id where m.commission_pct > 0.15;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});


app.get("/manager_job_titles", async (req, res) => {
    try {
        const result = await pool.query(
            "select distinct j.job_title from jobs j inner join employees e on j.job_id = e.job_id where e.employee_id in (select distinct manager_id from employees where manager_id is not null);"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});


app.get("/postal_codes_in_asia", async (req, res) => {
    try {
        const result = await pool.query(
            "select l.postal_code from locations l inner join countries c on l.country_id = c.country_id inner join regions r on c.region_id = r.region_id where r.region_name = 'Asia';"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/departments_below_avg_commission", async (req, res) => {
    try {
        const result = await pool.query(
            "select distinct d.department_name from employees e inner join departments d on e.department_id = d.department_id where e.commission_pct is not null and e.commission_pct < (select avg(commission_pct) from employees where commission_pct is not null);"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});
app.get("/high_salary_employees", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.first_name, e.last_name, j.job_title from employees e join jobs j on e.job_id = j.job_id where e.salary > ( select avg(salary) from employees where department_id = e.department_id);");

        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/no_department_employees", async (req, res) => {
    try {
        const result = await pool.query(
            "select employee_id, first_name, last_name from employees where department_id is null;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});
app.get("/multiple_jobs_employees", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.first_name, e.last_name from employees e join job_history jh on e.employee_id = jh.employee_id group by e.employee_id having count(jh.job_id) > 1;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/employee_count_by_department", async (req, res) => {
    try {
        const result = await pool.query(
            "select d.department_name, count(e.employee_id) as employee_count from departments d left join employees e on d.department_id = e.department_id group by d.department_name;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/total_salary_by_job", async (req, res) => {
    try {
        const result = await pool.query(
            "select j.job_title, sum(e.salary) as total_salary from employees e join jobs j on e.job_id = j.job_id group by j.job_title;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});
app.get("/avg_commission_by_department", async (req, res) => {
    try {
        const result = await pool.query(
            "select d.department_name, avg(e.commission_pct) as avg_commission from departments d join employees e on d.department_id = e.department_id group by d.department_name;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});
app.get("/max_salary_by_country", async (req, res) => {
    try {
        const result = await pool.query(
            "select c.country_name, max(e.salary) as max_salary from employees e join departments d on e.department_id = d.department_id join locations l on d.location_id = l.location_id join countries c on l.country_id = c.country_id group by c.country_name;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/employees_with_z", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.first_name, e.last_name, d.department_name, l.city, l.state_province from employees e join departments d on e.department_id = d.department_id join locations l on d.location_id = l.location_id where lower(e.first_name) like '%z%';"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/jobs_in_date_range", async (req, res) => {
    try {
        const result = await pool.query(
            "select j.job_title, d.department_name, e.first_name || ' ' || e.last_name as full_name, jh.start_date from job_history jh  join employees e on jh.employee_id = e.employee_id join jobs j on jh.job_id = j.job_id join departments d on jh.department_id = d.department_id where jh.start_date >= '1993-01-01' and jh.end_date <= '1997-08-31';"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});
app.get("/departments_with_two_employees", async (req, res) => {
    try {
        const result = await pool.query(
            "select c.country_name, l.city, count(distinct d.department_id) as department_count from employees e join departments d on e.department_id = d.department_id join locations l on d.location_id = l.location_id join countries c on l.country_id = c.country_id group by c.country_name, l.city having count(e.employee_id) >= 2;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});
app.get("/no_commission_last_jobs", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.first_name, e.last_name, j.job_title, jh.start_date, jh.end_date from job_history jh inner join employees e on e.employee_id = jh.employee_id  inner join jobs j on j.job_id = jh.job_id where e.commission_pct is null;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});
app.get("/employee_country", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.employee_id, e.first_name, e.last_name, c.country_name from employees e inner join departments d on d.department_id = e.department_id inner join locations l on l.location_id = d.location_id inner join countries c on c.country_id = l.country_id;"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/lowest_salary_employee", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.first_name, e.last_name, e.salary, e.department_id from employees e where e.salary = (select min(salary) from employees where department_id = e.department_id)"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/third_highest_salary", async (req, res) => {
    try {
        const result = await pool.query(
            "select * from employees where salary = ( select distinct salary from employees order by salary desc offset 2 limit 1 )"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get("/above_avg_salary_j_name", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.employee_id, e.first_name, e.last_name, e.salary from employees e where e.salary > (select avg(salary) from employees ) and e.department_id in ( select department_id from employees where lower(first_name) like '%j%' or lower(last_name) like '%j%' )"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});
app.get("/employees_toronto", async (req, res) => {
    try {
        const result = await pool.query(
            "select e.employee_id, e.first_name, e.last_name, j.job_title from employees e inner join jobs j on j.job_id = e.job_id inner join departments d on d.department_id = e.department_id inner join locations l on l.location_id = d.location_id where lower(l.city) = 'toronto'"
        );
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
});

app.get('/students', async (req, res) => {
    try {
        const result = await pool.query('select * from student');
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
})

app.get('/gettotalstd', async (req, res) => {
    try {
        const result = await pool.query('select count(ID) from student');
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
})

app.get("/job_history", async (req, res) => {
    try {
        const result = await pool.query("select jh.*,e.first_name,e.last_name,c.country_name,j.job_title from job_history jh inner join employees e on e.employee_id = jh.employee_id inner join jobs j on j.job_id = jh.job_id inner join departments d on d.department_id = e.department_id inner join locations l on l.location_id = d.location_id inner join countries c on c.country_id = l.country_id ;");
        if (!result) return res.status(401).json({ msg: "incorrect query" })
        return res.status(200).json({ data: result.rows })
    } catch (err) {
        res.status(500).json({ Error: err.message })
    }
})

app.get("/regions_countries_locations", async (req, res) => {
    try {
        const result = await pool.query(
            "select r.*, c.country_name, l.city, l.location_id from regions r inner join countries c ON c.region_id = r.region_id inner join locations l ON l.country_id = c.country_id;");
        if (!result) return res.status(401).json({ msg: "incorrect query" });
        return res.status(200).json({ data: result.rows });
    } catch (err) {
        res.status(500).json({ Error: err.message });
    }
})

const PORT = process.env.PORT;
app.listen(PORT, () => {
    console.log(`Connected Successfully....Running on PORT ${PORT}`);
});