test_that("cf mirai local basic", {
  skip_if_not_installed("mirai")

  reg = makeTestRegistry()
  reg$cluster.functions = makeClusterFunctionsMiraiLocal(ncpus = 2, fs.latency = 0)

  ids = batchMap(Sys.sleep, time = c(1, 1), reg = reg)
  silent({
    submitJobs(1:2, reg = reg)
    expect_equal(findOnSystem(reg = reg), findJobs(reg = reg))
    expect_true(waitForJobs(sleep = 0.2, expire.after = 1, reg = reg))
  })
  expect_data_table(findOnSystem(reg = reg), nrows = 0)
  expect_equal(findDone(reg = reg), findJobs(reg = reg))

  # Clean up
 mirai::daemons(0, .compute = "batchtools")
})

test_that("cf mirai external daemons basic", {
  skip_if_not_installed("mirai")

  # Set up external daemons
  mirai::daemons(n = 2)

  reg = makeTestRegistry()
  reg$cluster.functions = makeClusterFunctionsMirai(fs.latency = 0)

  ids = batchMap(Sys.sleep, time = c(1, 1), reg = reg)
  silent({
    submitJobs(1:2, reg = reg)
    expect_equal(findOnSystem(reg = reg), findJobs(reg = reg))
    expect_true(waitForJobs(sleep = 0.2, expire.after = 1, reg = reg))
  })
  expect_data_table(findOnSystem(reg = reg), nrows = 0)
  expect_equal(findDone(reg = reg), findJobs(reg = reg))

  # Clean up
  mirai::daemons(0)
})

test_that("cf mirai kill jobs", {
  skip_if_not_installed("mirai")

  mirai::daemons(n = 4)

  reg = makeTestRegistry()
  reg$cluster.functions = makeClusterFunctionsMirai(fs.latency = 0)

  ids = batchMap(Sys.sleep, time = rep(30, 4), reg = reg)
  silent({
    submitJobs(reg = reg)
  })
  Sys.sleep(0.5)

  # Should have 4 running jobs
  expect_data_table(findRunning(reg = reg), nrows = 4)

  # Kill one job
  silent({
    killJobs(ids = 1, reg = reg)
  })
  Sys.sleep(0.5)

  # Should have 3 running jobs
  expect_data_table(findRunning(reg = reg), nrows = 3)

  # Kill remaining jobs
  silent({
    killJobs(reg = reg)
  })
  Sys.sleep(0.5)

  # Should have 0 running jobs
  expect_data_table(findRunning(reg = reg), nrows = 0)

  # Clean up
  mirai::daemons(0)
})

test_that("cf mirai error without daemons", {
  skip_if_not_installed("mirai")

  # Make sure no daemons are set
  mirai::daemons(0)

  reg = makeTestRegistry()
  reg$cluster.functions = makeClusterFunctionsMirai(fs.latency = 0)

  ids = batchMap(identity, 1, reg = reg)

  # Should error because daemons are not set
  expect_error(submitJobs(reg = reg), "daemons are not set up")
})
