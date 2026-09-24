const { isBotContributor } = require('./contributor_filters');
const formatDate = date => [date.getFullYear(), String(date.getMonth() + 1).padStart(2, '0'), String(date.getDate()).padStart(2, '0')].join('-');

async function fetchRepoStatsViaGraphQL(repoName, startDate, endDate, graphQLClient, orgName) {
    const startDateStr = formatDate(startDate);
    const endDateStr = formatDate(endDate);

    console.log(`[GraphQL] Fetching ${repoName} stats from ${startDateStr} to ${endDateStr}...`);

    const statsMap = new Map();
    const contributorDetailsMap = new Map(); // 新增：保存每天的贡献者详情
    const currentDate = new Date(startDate);
    while (currentDate <= endDate) {
        const dateKey = formatDate(currentDate);
        statsMap.set(dateKey, {
            new_prs: 0,
            closed_merged_prs: 0,
            new_issues: 0,
            closed_issues: 0,
            active_contributors: new Set(),
        });
        contributorDetailsMap.set(dateKey, new Map()); // 每天的贡献者详情
        currentDate.setDate(currentDate.getDate() + 1);
    }

    const query = `
        query RepoStats($owner: String!, $repo: String!, $prCursor: String, $issueCursor: String) {
            repository(owner: $owner, name: $repo) {
                pullRequests(first: 100, after: $prCursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
                    totalCount
                    pageInfo { hasNextPage endCursor }
                    nodes {
                        createdAt
                        updatedAt
                        closedAt
                        mergedAt
                        state
                        author {
                            login
                            avatarUrl
                            ... on User {
                                databaseId
                            }
                        }
                    }
                }
                issues(first: 100, after: $issueCursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
                    totalCount
                    pageInfo { hasNextPage endCursor }
                    nodes {
                        createdAt
                        updatedAt
                        closedAt
                        state
                        author {
                            login
                            avatarUrl
                            ... on User {
                                databaseId
                            }
                        }
                    }
                }
            }
        }
    `;

    try {
        // Fetch PRs with pagination
        let prCursor = null;
        let prDone = false;
        let totalPrsFetched = 0;

        while (!prDone) {
            const data = await graphQLClient(query, {
                owner: orgName,
                repo: repoName,
                prCursor: prCursor,
                issueCursor: null,
            });

            if (!data?.repository) {
                throw new Error(`Repository ${repoName} not found or inaccessible.`);
            }

            const prs = data.repository.pullRequests;
            totalPrsFetched += prs.nodes.length;

            for (const pr of prs.nodes) {
                const createdDate = pr.createdAt ? pr.createdAt.split('T')[0] : null;
                const closedDate = pr.closedAt ? pr.closedAt.split('T')[0] : null;

                if (createdDate && createdDate >= startDateStr && createdDate <= endDateStr) {
                    if (statsMap.has(createdDate)) {
                        statsMap.get(createdDate).new_prs++;
                        if (pr.author?.login && !isBotContributor(pr.author.login)) {
                            const username = pr.author.login;
                            statsMap.get(createdDate).active_contributors.add(username);

                            // 保存贡献者详情
                            const dayContributors = contributorDetailsMap.get(createdDate);
                            if (!dayContributors.has(username)) {
                                dayContributors.set(username, {
                                    username,
                                    avatar_url: pr.author.avatarUrl || null,
                                    github_id: pr.author.databaseId || null,
                                    prs_opened: 0,
                                    prs_closed: 0,
                                    issues_opened: 0,
                                    issues_closed: 0
                                });
                            }
                            dayContributors.get(username).prs_opened++;
                        }
                    }
                }

                if (closedDate && closedDate >= startDateStr && closedDate <= endDateStr) {
                    if (statsMap.has(closedDate)) {
                        statsMap.get(closedDate).closed_merged_prs++;
                        if (pr.author?.login && !isBotContributor(pr.author.login)) {
                            const username = pr.author.login;

                            // 保存贡献者详情
                            const dayContributors = contributorDetailsMap.get(closedDate);
                            if (!dayContributors.has(username)) {
                                dayContributors.set(username, {
                                    username,
                                    avatar_url: pr.author.avatarUrl || null,
                                    github_id: pr.author.databaseId || null,
                                    prs_opened: 0,
                                    prs_closed: 0,
                                    issues_opened: 0,
                                    issues_closed: 0
                                });
                            }
                            dayContributors.get(username).prs_closed++;
                        }
                    }
                }

                if (pr.updatedAt && pr.updatedAt.split('T')[0] < startDateStr) {
                    prDone = true;
                    break;
                }
            }

            if (prs.pageInfo.hasNextPage && !prDone) {
                if (!prs.pageInfo.endCursor || prs.pageInfo.endCursor === prCursor) throw new Error('Invalid PR pagination cursor');
                prCursor = prs.pageInfo.endCursor;
            } else {
                prDone = true;
            }
        }

        // Fetch Issues with pagination
        let issueCursor = null;
        let issueDone = false;
        let totalIssuesFetched = 0;

        while (!issueDone) {
            const data = await graphQLClient(query, {
                owner: orgName,
                repo: repoName,
                prCursor: null,
                issueCursor: issueCursor,
            });

            if (!data?.repository) {
                throw new Error(`Repository ${repoName} not found or inaccessible.`);
            }

            const issues = data.repository.issues;
            totalIssuesFetched += issues.nodes.length;

            for (const issue of issues.nodes) {
                const createdDate = issue.createdAt ? issue.createdAt.split('T')[0] : null;
                const closedDate = issue.closedAt ? issue.closedAt.split('T')[0] : null;

                if (createdDate && createdDate >= startDateStr && createdDate <= endDateStr) {
                    if (statsMap.has(createdDate)) {
                        statsMap.get(createdDate).new_issues++;
                        if (issue.author?.login && !isBotContributor(issue.author.login)) {
                            const username = issue.author.login;
                            statsMap.get(createdDate).active_contributors.add(username);

                            // 保存贡献者详情
                            const dayContributors = contributorDetailsMap.get(createdDate);
                            if (!dayContributors.has(username)) {
                                dayContributors.set(username, {
                                    username,
                                    avatar_url: issue.author.avatarUrl || null,
                                    github_id: issue.author.databaseId || null,
                                    prs_opened: 0,
                                    prs_closed: 0,
                                    issues_opened: 0,
                                    issues_closed: 0
                                });
                            }
                            dayContributors.get(username).issues_opened++;
                        }
                    }
                }

                if (closedDate && closedDate >= startDateStr && closedDate <= endDateStr) {
                    if (statsMap.has(closedDate)) {
                        statsMap.get(closedDate).closed_issues++;
                        if (issue.author?.login && !isBotContributor(issue.author.login)) {
                            const username = issue.author.login;

                            // 保存贡献者详情
                            const dayContributors = contributorDetailsMap.get(closedDate);
                            if (!dayContributors.has(username)) {
                                dayContributors.set(username, {
                                    username,
                                    avatar_url: issue.author.avatarUrl || null,
                                    github_id: issue.author.databaseId || null,
                                    prs_opened: 0,
                                    prs_closed: 0,
                                    issues_opened: 0,
                                    issues_closed: 0
                                });
                            }
                            dayContributors.get(username).issues_closed++;
                        }
                    }
                }

                if (issue.updatedAt && issue.updatedAt.split('T')[0] < startDateStr) {
                    issueDone = true;
                    break;
                }
            }

            if (issues.pageInfo.hasNextPage && !issueDone) {
                if (!issues.pageInfo.endCursor || issues.pageInfo.endCursor === issueCursor) throw new Error('Invalid issue pagination cursor');
                issueCursor = issues.pageInfo.endCursor;
            } else {
                issueDone = true;
            }
        }

        console.log(`[GraphQL] ${repoName}: Fetched ${totalPrsFetched} PRs and ${totalIssuesFetched} Issues.`);

        // 返回统计数据和贡献者详情
        return { statsMap, contributorDetailsMap };

    } catch (error) {
        throw new Error(`[GraphQL] Failed to fetch stats for ${repoName}: ${error.message}`, { cause: error });
    }
}

module.exports = { fetchRepoStatsViaGraphQL };
